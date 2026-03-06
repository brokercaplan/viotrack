""" VioTrack Scraper - Miami Beach SM Portal
Uses requests for address lookups, Playwright for JS-rendered city feed agendas.
"""
import json
import re
import time
import traceback
from datetime import datetime, date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ──────────────────────────────────────────────────────────────────
ADDRESS_INQUIRY_URL = "https://apps.miamibeachfl.gov/SMAddressInquire/"
AGENDA_LIST_URL    = "https://apps.miamibeachfl.gov/energovagenda/Public/"
DATA_FILE = Path("data/violations.json")

WATCH_ADDRESSES = [
    "411 WASHINGTON AVE",
    "1941 LIBERTY AVE",
]

STREET_TYPES = {
    "AVE","AVENUE","ST","STREET","BLVD","BOULEVARD",
    "DR","DRIVE","RD","ROAD","LN","LANE","CT","COURT",
    "PL","PLACE","WAY","TER","TERRACE","CIR","CIRCLE","HWY","HIGHWAY"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VioTrack/1.0)",
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
}

# Only include cases with these statuses
OPEN_STATUSES = {"open", "active", "pending", "scheduled", "new", "appealed", "continued"}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ── Helpers ──────────────────────────────────────────────────────────────────
def parse_address(raw):
    parts = raw.strip().upper().split()
    if not parts:
        return None, None
    number = parts[0]
    name_parts = [p for p in parts[1:] if p not in STREET_TYPES]
    return number, " ".join(name_parts)

def parse_dollar(text):
    cleaned = re.sub(r"[^\d.]", "", text or "")
    try:
        return int(float(cleaned))
    except:
        return 0

def classify_type(text):
    t = text.upper()
    if "ELECTRICAL" in t or "BVE" in t: return "Building – Electrical"
    if "PLUMBING"   in t or "BVP" in t: return "Building – Plumbing"
    if "MECHANICAL" in t or "BVM" in t: return "Building – Mechanical"
    if "UNSAFE"     in t:               return "Unsafe Structures"
    if "RECERT"     in t or "EBR" in t: return "40-Yr Recertification"
    if "COMBO"      in t or "BVC" in t: return "Building – Combo"
    if "MAINTENANCE" in t:              return "Property Maintenance"
    if "CODE"       in t:               return "City Code Violation"
    if "PARKING"    in t or "PV" in t:  return "Parking Violation"
    if "FIRE"       in t:               return "Fire Violation"
    if "ROW"        in t or "RIGHT" in t: return "Right-of-Way"
    if "NOISE"      in t:               return "Noise Violation"
    return text.strip() or "Violation"

def parse_hearing_date(text):
    """Parse date strings like '3/19/2026' or 'Thursday, March 19, 2026'."""
    text = text.strip()
    for fmt in ("%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except:
            pass
    m = re.search(r"(\w+ \d+, \d{4})", text)
    if m:
        try:
            return datetime.strptime(m.group(1), "%B %d, %Y").date()
        except:
            pass
    return None

def is_future_or_today(date_text):
    d = parse_hearing_date(date_text)
    if d is None:
        return False
    return d >= date.today()

def load_data():
    if DATA_FILE.exists():
        return json.loads(DATA_FILE.read_text())
    return {"myProperties": [], "cityFeed": [], "watchAddresses": WATCH_ADDRESSES, "lastUpdated": None}

def save_data(data):
    data["lastUpdated"] = datetime.utcnow().isoformat() + "Z"
    DATA_FILE.write_text(json.dumps(data, indent=2))
    print(f"Saved {DATA_FILE}")

# ── Part 1: Address inquiry (requests) ───────────────────────────────────────
def scrape_address(raw_address):
    street_num, street_name = parse_address(raw_address)
    if not street_num or not street_name:
        print(f"  Could not parse: {raw_address}")
        return []
    print(f"  Scraping: {raw_address}")
    try:
        resp = SESSION.get(ADDRESS_INQUIRY_URL, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        token_el = soup.find("input", {"name": "__RequestVerificationToken"})
        token = token_el["value"] if token_el else ""
        payload = {
            "__RequestVerificationToken": token,
            "StreetNbr":    street_num,
            "StreeName":    street_name,
            "ViolationType": "%",
        }
        resp2 = SESSION.post(ADDRESS_INQUIRY_URL, data=payload, timeout=15)
        resp2.raise_for_status()
        soup2 = BeautifulSoup(resp2.text, "html.parser")
        table = soup2.find("table")
        if not table:
            print(f"  No results for {raw_address}")
            return []
        cases = []
        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 5:
                continue
            status = cells[4] if len(cells) > 4 else ""
            if status.lower() not in OPEN_STATUSES:
                continue
            case_num  = cells[1] if len(cells) > 1 else "—"
            dept_viol = cells[2] if len(cells) > 2 else "—"
            viol_type = cells[3] if len(cells) > 3 else "—"
            owner     = cells[5] if len(cells) > 5 else "—"
            balance   = parse_dollar(cells[7]) if len(cells) > 7 else 0
            hearing   = cells[0].split()[0] if cells[0] else "—"
            cases.append({
                "id":          abs(hash(case_num)) % 999999,
                "property":    raw_address,
                "caseNum":     case_num,
                "deptViol":    dept_viol,
                "type":        classify_type(viol_type),
                "status":      status,
                "hearing":     hearing,
                "balance":     balance,
                "dailyFine":   None,
                "owner":       owner,
                "description": viol_type,
                "code":        "",
            })
        print(f"    Found {len(cases)} open cases for {raw_address}")
        return cases
    except Exception as e:
        print(f"  Error scraping {raw_address}: {e}")
        traceback.print_exc()
        return []

# ── Part 2: City Feed via Playwright ─────────────────────────────────────────
def scrape_city_feed():
    """
    Use Playwright to:
    1. Load the agenda listing page
    2. Find all upcoming hearings (today or future)
    3. For each, click View Agenda, wait for SSRS report to render
    4. Extract all cases from the rendered DOM across all pages
    5. Filter out cases with unit numbers (unit-specific violations)
    """
    print("\nScraping city agenda feed with Playwright...")
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        print("  Playwright not installed - skipping city feed")
        return []

    cases = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(30000)

        try:
            # Load agenda list
            page.goto(AGENDA_LIST_URL, wait_until="networkidle")
            print("  Loaded agenda listing")

            # Find all View Agenda links and their dates
            rows = page.query_selector_all("table tr")
            upcoming = []
            for row in rows[1:]:  # skip header
                cells = row.query_selector_all("td")
                if len(cells) < 4:
                    continue
                date_text = cells[1].inner_text().strip() if len(cells) > 1 else ""
                desc_text = cells[3].inner_text().strip() if len(cells) > 3 else ""
                link = cells[0].query_selector("a")
                if link and is_future_or_today(date_text):
                    upcoming.append({
                        "date": date_text,
                        "desc": desc_text,
                        "link": link,
                    })

            print(f"  Found {len(upcoming)} upcoming hearing(s)")

            for hearing in upcoming:
                print(f"  Processing: {hearing['date']} - {hearing['desc']}")
                try:
                    hearing["link"].click()
                    # Wait for SSRS report to finish loading
                    page.wait_for_selector("text=Special Master Case#", timeout=20000)
                    time.sleep(2)  # let report fully render

                    hearing_cases = extract_cases_from_ssrs(page, hearing["date"])
                    print(f"    Extracted {len(hearing_cases)} cases")
                    cases.extend(hearing_cases)

                    # Go back to the listing
                    page.goto(AGENDA_LIST_URL, wait_until="networkidle")
                    time.sleep(1)

                except PWTimeout:
                    print(f"    Timeout waiting for report on {hearing['date']} - skipping")
                except Exception as e:
                    print(f"    Error on {hearing['date']}: {e}")

        finally:
            browser.close()

    print(f"  City feed total: {len(cases)} cases")
    return cases

def extract_cases_from_ssrs(page, hearing_date):
    """
    Extract all cases from the SSRS report rendered in the browser.
    The report may span multiple pages - iterate through all pages.
    Only include cases where the property address has no unit number.
    """
    from playwright.sync_api import TimeoutError as PWTimeout
    all_cases = []
    page_num = 1

    while True:
        # Get current page text
        try:
            content = page.inner_text("body")
        except:
            break

        # Parse cases from this page
        page_cases = parse_ssrs_text(content, hearing_date)
        all_cases.extend(page_cases)

        # Check if there's a next page button that's enabled
        try:
            next_btn = page.query_selector("input[title='Next Page'], a[title='Next Page']")
            if next_btn:
                is_disabled = next_btn.get_attribute("disabled")
                if is_disabled:
                    break
                next_btn.click()
                page.wait_for_timeout(2000)
                page_num += 1
            else:
                # Try clicking the > navigation button in SSRS toolbar
                btns = page.query_selector_all("img[src*='NextPage']")
                active_next = None
                for btn in btns:
                    if "Disabled" not in (btn.get_attribute("src") or ""):
                        active_next = btn
                        break
                if active_next:
                    active_next.click()
                    page.wait_for_timeout(2000)
                    page_num += 1
                else:
                    break
        except PWTimeout:
            break
        except:
            break

        if page_num > 30:  # safety cap
            break

    return all_cases

def parse_ssrs_text(text, hearing_date):
    """
    Parse the inner text of the SSRS report page.
    Split on 'Special Master Case#' boundaries.
    Filter out cases with unit numbers in the address.
    """
    # Split into case blocks
    blocks = re.split(r"(?=Special Master Case#\s*\n)", text, flags=re.I)
    cases = []
    for block in blocks:
        if "Special Master Case#" not in block:
            continue
        c = extract_case_from_block(block, hearing_date)
        if c:
            cases.append(c)
    return cases

def extract_case_from_block(block, hearing_date):
    lines = [l.strip() for l in block.split("\n") if l.strip()]

    def next_after(keyword):
        for i, line in enumerate(lines):
            if keyword.lower() in line.lower():
                for j in range(i+1, min(i+4, len(lines))):
                    if lines[j] and not any(k in lines[j].lower() for k in
                       ["special master", "department", "property", "description",
                        "inspector", "status", "violation type", "code", "comments"]):
                        return lines[j]
        return "—"

    # Case number
    case_num = next_after("Special Master Case#")
    if case_num == "—" or not re.match(r"SM[A-Z]d{4}-d+", case_num, re.I):
        return None

    # Property address - find it and check for unit numbers
    address = next_after("Property Address:")
    if address == "—":
        address = "MIAMI BEACH"
    else:
        # Filter out addresses with unit numbers
        # Unit patterns: "Unit:", "Apt", "#", "Ste", or address starting with 0
        if re.search(r"\bUnit\b|\bApt\b|\bSte\b|\bSuite\b|\b#\d", address, re.I):
            return None
        if address.startswith("0 ") or address == "0":
            return None

    dept_viol  = next_after("Department Violation #")
    owner      = next_after("miller") or next_after("c/o") or "—"
    desc       = next_after("Description:")
    status_raw = ""
    for line in lines:
        if line.lower().startswith("status:"):
            status_raw = line.split(":", 1)[1].strip()
            break

    # Fine amount
    fine = 0
    for line in lines:
        m = re.search(r"\$(\d[\d,]*)", line)
        if m:
            fine = parse_dollar(m.group(1))
            break

    viol_type = next_after("Violation Type:")

    return {
        "id":          abs(hash(case_num)) % 999999,
        "property":    address.upper().strip(),
        "caseNum":     case_num,
        "deptViol":    dept_viol,
        "type":        classify_type(dept_viol + " " + viol_type),
        "status":      status_raw or "Scheduled",
        "hearing":     hearing_date,
        "balance":     fine,
        "dailyFine":   None,
        "owner":       owner.strip(),
        "description": (desc.strip()[:200] if desc != "—" else ""),
        "dateAdded":   datetime.utcnow().strftime("%Y-%m-%d"),
        "contacted":   False,
        "source":      "cityFeed",
    }

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("VioTrack Scraper — Miami Beach SM Portal")
    print(f"Started: {datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    data = load_data()

    # Part 1: Watched addresses
    print("\n[1/2] Scraping watched addresses...")
    all_my_cases = []
    for addr in data.get("watchAddresses", WATCH_ADDRESSES):
        all_my_cases.extend(scrape_address(addr))
        time.sleep(2)

    if all_my_cases:
        existing = {c["caseNum"]: c for c in data.get("myProperties", [])}
        for nc in all_my_cases:
            cn = nc["caseNum"]
            if cn in existing:
                existing[cn].update({"status": nc["status"], "balance": nc["balance"], "hearing": nc["hearing"]})
            else:
                existing[cn] = nc
        data["myProperties"] = list(existing.values())
        print(f"  My Properties: {len(data['myProperties'])} total open cases")
    else:
        print("  No cases found — keeping existing data")

    # Part 2: City feed
    print("\n[2/2] Scraping city agenda feed...")
    city_cases = scrape_city_feed()

    if city_cases:
        existing_city = {c["caseNum"]: c for c in data.get("cityFeed", [])}
        for nc in city_cases:
            cn = nc["caseNum"]
            if cn not in existing_city:
                existing_city[cn] = nc
            else:
                existing_city[cn].update({"balance": nc["balance"], "status": nc["status"], "hearing": nc["hearing"]})
        data["cityFeed"] = list(existing_city.values())
        print(f"  City Feed: {len(data['cityFeed'])} total cases")
    else:
        print("  No city cases found — keeping existing data")

    save_data(data)
    print(f"\nDone. Updated {DATA_FILE}")

if __name__ == "__main__":
    main()

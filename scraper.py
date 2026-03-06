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
AGENDA_SCHED_URL   = "https://apps.miamibeachfl.gov/energovagenda/Public/AgendaSchedules"
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
    if "ELECTRICAL" in t or "BVE" in t: return "Building - Electrical"
    if "PLUMBING"   in t or "BVP" in t: return "Building - Plumbing"
    if "MECHANICAL" in t or "BVM" in t: return "Building - Mechanical"
    if "UNSAFE"     in t:               return "Unsafe Structures"
    if "RECERT"     in t or "EBR" in t: return "40-Yr Recertification"
    if "COMBO"      in t or "BVC" in t: return "Building - Combo"
    if "MAINTENANCE" in t:              return "Property Maintenance"
    if "CODE"       in t:               return "City Code Violation"
    if "PARKING"    in t or "PV" in t:  return "Parking Violation"
    if "FIRE"       in t:               return "Fire Violation"
    if "ROW"        in t or "RIGHT" in t: return "Right-of-Way"
    if "NOISE"      in t:               return "Noise Violation"
    return text.strip() or "Violation"

def parse_hearing_date(text):
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
    return d is not None and d >= date.today()

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
                "id": abs(hash(case_num)) % 999999,
                "property": raw_address,
                "caseNum": case_num,
                "deptViol": dept_viol,
                "type": classify_type(viol_type),
                "status": status,
                "hearing": hearing,
                "balance": balance,
                "dailyFine": None,
                "owner": owner,
                "description": viol_type,
                "code": "",
            })
        print(f"    Found {len(cases)} open cases")
        return cases
    except Exception as e:
        print(f"  Error: {e}")
        return []

# ── Part 2: City Feed via Playwright ─────────────────────────────────────────
def scrape_city_feed():
    print("\nScraping city agenda feed with Playwright...")
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        print("  Playwright not installed")
        return []

    cases = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(30000)

        try:
            # Load agenda list and collect upcoming hearing info (index + date + desc)
            page.goto(AGENDA_LIST_URL, wait_until="networkidle")
            rows = page.query_selector_all("table tr")

            upcoming = []
            for i, row in enumerate(rows[1:], start=0):  # row index for postback
                cells = row.query_selector_all("td")
                if len(cells) < 2:
                    continue
                date_text = cells[1].inner_text().strip()
                desc_text = cells[3].inner_text().strip() if len(cells) > 3 else ""
                if is_future_or_today(date_text):
                    upcoming.append({"idx": i, "date": date_text, "desc": desc_text})

            print(f"  Found {len(upcoming)} upcoming hearing(s)")

            for hearing in upcoming:
                print(f"  Processing: {hearing['date']} - {hearing['desc']}")
                try:
                    # Re-load the listing page fresh each time to avoid stale elements
                    page.goto(AGENDA_LIST_URL, wait_until="networkidle")
                    # Click the View Agenda link for this row by index
                    all_links = page.query_selector_all("table tr td:first-child a")
                    if hearing["idx"] < len(all_links):
                        all_links[hearing["idx"]].click()
                        # Wait for the SSRS report to render
                        try:
                            page.wait_for_selector("text=Special Master Case#", timeout=25000)
                        except PWTimeout:
                            # Try waiting for the AgendaSchedules URL instead
                            page.wait_for_url("**/AgendaSchedules**", timeout=10000)
                            page.wait_for_timeout(3000)

                        hearing_cases = extract_all_report_pages(page, hearing["date"])
                        print(f"    Extracted {len(hearing_cases)} cases")
                        cases.extend(hearing_cases)
                    else:
                        print(f"    Link index {hearing['idx']} out of range ({len(all_links)} links)")
                except Exception as e:
                    print(f"    Error on {hearing['date']}: {e}")

        finally:
            browser.close()

    print(f"  City feed total: {len(cases)} cases")
    return cases

def extract_all_report_pages(page, hearing_date):
    """Extract cases from all pages of the SSRS report."""
    from playwright.sync_api import TimeoutError as PWTimeout
    all_cases = []
    page_num = 1

    while True:
        content = page.inner_text("body")
        page_cases = parse_ssrs_page(content, hearing_date)
        all_cases.extend(page_cases)
        print(f"      Page {page_num}: {len(page_cases)} cases")

        # Try to click Next Page in the SSRS toolbar
        # Active next page button has src NOT containing "Disabled"
        try:
            next_imgs = page.query_selector_all("img[src*='NextPage']")
            active_next = None
            for img in next_imgs:
                src = img.get_attribute("src") or ""
                if "Disabled" not in src:
                    active_next = img
                    break
            if active_next:
                active_next.click()
                page.wait_for_timeout(2500)
                page_num += 1
            else:
                break
        except:
            break

        if page_num > 50:
            break

    return all_cases

def parse_ssrs_page(text, hearing_date):
    """Parse one page of SSRS report text. Split on case boundaries."""
    # Normalize whitespace but keep newlines
    text = re.sub(r"[ \t]+", " ", text)

    # Split on each case block - case# is on its own line after the label
    blocks = re.split(r"Special Master Case#", text, flags=re.I)
    cases = []
    for block in blocks[1:]:  # skip everything before first case
        c = extract_case_from_block(block, hearing_date)
        if c:
            cases.append(c)
    return cases

def extract_case_from_block(block, hearing_date):
    """Extract structured case data from a text block following 'Special Master Case#'."""
    lines = [l.strip() for l in block.split("\n") if l.strip()]
    if not lines:
        return None

    # First non-empty line should be the case number
    case_num = lines[0].strip()
    if not re.match(r"SM[A-Z]\d{4}-\d+", case_num, re.I):
        # Try second line
        if len(lines) > 1 and re.match(r"SM[A-Z]\d{4}-\d+", lines[1], re.I):
            case_num = lines[1]
        else:
            return None

    def find_after(keyword, lines):
        for i, line in enumerate(lines):
            if keyword.lower() in line.lower():
                for j in range(i+1, min(i+5, len(lines))):
                    val = lines[j].strip()
                    if val and not any(k in val.lower() for k in [
                        "special master", "department violation", "property address",
                        "description:", "inspector", "status:", "violation type:",
                        "code", "comments:", "area", "c/o", "miller"
                    ]):
                        return val
        return "—"

    # Property address
    address = find_after("Property Address:", lines)
    if address == "—":
        # Try inline: "Property Address: 123 MAIN ST"
        for line in lines:
            m = re.search(r"Property Address:s*(.+)", line, re.I)
            if m:
                address = m.group(1).strip()
                break

    # Filter: skip if address has unit numbers
    if address and address != "—":
        if re.search(r"\bUnit\b|\bApt\b|\bSte\b|\bSuite\b|\bUnit:\b", address, re.I):
            return None
        if re.match(r"^0\s", address) or address.strip() == "0":
            return None
    else:
        address = "MIAMI BEACH"

    dept_viol = find_after("Department Violation #", lines) or find_after("Department Violation", lines)
    status_raw = "Scheduled"
    for line in lines:
        m = re.match(r"Status:\s*(.+)", line, re.I)
        if m:
            status_raw = m.group(1).strip()
            break

    desc = find_after("Description:", lines)
    viol_type = find_after("Violation Type:", lines)

    fine = 0
    for line in lines:
        m = re.search(r"\$(\d[\d,]*)", line)
        if m:
            fine = parse_dollar(m.group(1))
            break

    # Owner: line after case number that looks like a name
    owner = "—"
    for line in lines[2:8]:
        if re.match(r"[A-Z][a-z].*\s[A-Z]|[A-Z ]{4,}", line) and "Address" not in line:
            owner = line
            break

    return {
        "id": abs(hash(case_num)) % 999999,
        "property": address.upper().strip(),
        "caseNum": case_num,
        "deptViol": dept_viol,
        "type": classify_type(dept_viol + " " + viol_type),
        "status": status_raw,
        "hearing": hearing_date,
        "balance": fine,
        "dailyFine": None,
        "owner": owner.strip(),
        "description": (desc.strip()[:200] if desc != "—" else ""),
        "dateAdded": datetime.utcnow().strftime("%Y-%m-%d"),
        "contacted": False,
        "source": "cityFeed",
    }

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("VioTrack Scraper — Miami Beach SM Portal")
    print(f"Started: {datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    data = load_data()

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
        print("  No city cases — keeping existing data")

    save_data(data)
    print(f"\nDone. Updated {DATA_FILE}")

if __name__ == "__main__":
    main()

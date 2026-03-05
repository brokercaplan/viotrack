"""
VioTrack Scraper
Scrapes Miami Beach Special Magistrate portal for:
  1. Cases on watched addresses (My Properties)
  2. Citywide violations from SM agendas (City Feed)
Writes results to data/violations.json
"""

import json
import re
import time
import traceback
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
ADDRESS_INQUIRY_URL = "https://apps.miamibeachfl.gov/smaddressinquire/"
AGENDA_URL          = "https://apps.miamibeachfl.gov/energovagenda/Public/"
DATA_FILE           = Path("data/violations.json")

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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_address(raw: str):
    """Split '411 WASHINGTON AVE' → ('411', 'WASHINGTON')"""
    parts = raw.strip().upper().split()
    if not parts:
        return None, None
    number = parts[0]
    name_parts = [p for p in parts[1:] if p not in STREET_TYPES]
    street_name = " ".join(name_parts)
    return number, street_name


def load_data():
    if DATA_FILE.exists():
        return json.loads(DATA_FILE.read_text())
    return {"myProperties": [], "cityFeed": [], "watchAddresses": WATCH_ADDRESSES}


def save_data(data):
    data["lastUpdated"] = datetime.utcnow().isoformat() + "Z"
    DATA_FILE.write_text(json.dumps(data, indent=2))
    print(f"✓ Saved data/violations.json")


# ── Part 1: Scrape watched addresses ─────────────────────────────────────────
def scrape_address(raw_address: str) -> list:
    """Query SM address inquiry portal and return list of cases."""
    street_num, street_name = parse_address(raw_address)
    if not street_num or not street_name:
        print(f"  ✗ Could not parse address: {raw_address}")
        return []

    print(f"  Scraping: {raw_address} → num={street_num}, name={street_name}")

    try:
        # GET the form to capture ASP.NET hidden fields
        resp = SESSION.get(ADDRESS_INQUIRY_URL, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        def val(name):
            el = soup.find("input", {"name": name})
            return el["value"] if el and el.get("value") else ""

        payload = {
            "__VIEWSTATE":          val("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION":    val("__EVENTVALIDATION"),
            "ctl00$MainContent$txtStreetNumber": street_num,
            "ctl00$MainContent$txtStreetName":   street_name,
            "ctl00$MainContent$ddlViolationType": "All Violations",
            "ctl00$MainContent$btnSearch": "Search",
        }

        resp2 = SESSION.post(ADDRESS_INQUIRY_URL, data=payload, timeout=15)
        resp2.raise_for_status()
        soup2 = BeautifulSoup(resp2.text, "html.parser")

        cases = []
        table = soup2.find("table", {"id": re.compile(r"GridView", re.I)})
        if not table:
            # Try any table with case data
            table = soup2.find("table", class_=re.compile(r"grid|result", re.I))

        if table:
            rows = table.find_all("tr")[1:]  # Skip header row
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 4:
                    cases.append({
                        "property":    raw_address,
                        "caseNum":     cells[0] if len(cells) > 0 else "—",
                        "deptViol":    cells[1] if len(cells) > 1 else "—",
                        "type":        cells[2] if len(cells) > 2 else "—",
                        "status":      cells[3] if len(cells) > 3 else "—",
                        "hearing":     cells[4] if len(cells) > 4 else "—",
                        "balance":     parse_dollar(cells[5]) if len(cells) > 5 else 0,
                        "dailyFine":   None,
                        "owner":       cells[6] if len(cells) > 6 else "—",
                        "description": cells[7] if len(cells) > 7 else "—",
                        "code":        "",
                    })
            print(f"  ✓ Found {len(cases)} cases for {raw_address}")
        else:
            print(f"  ⚠ No results table found for {raw_address}")

        return cases

    except Exception as e:
        print(f"  ✗ Error scraping {raw_address}: {e}")
        traceback.print_exc()
        return []


def parse_dollar(text: str) -> int:
    """'$17,836.00' → 17836"""
    cleaned = re.sub(r"[^\d.]", "", text)
    try:
        return int(float(cleaned))
    except:
        return 0


# ── Part 2: Scrape citywide agenda feed ──────────────────────────────────────
def scrape_city_feed() -> list:
    """
    Pull upcoming agenda hearing dates and parse PDFs for all cases.
    Returns list of violation dicts for the City Feed tab.
    """
    print("\nScraping city agenda feed...")
    cases = []

    try:
        resp = SESSION.get(AGENDA_URL, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find PDF links in the agenda page
        pdf_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "docmgmt" in href or ".pdf" in href.lower() or "edoc" in href.lower():
                if not href.startswith("http"):
                    href = "https://apps.miamibeachfl.gov" + href
                pdf_links.append(href)

        # Also look for agenda date links that lead to PDFs
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "agenda" in href.lower() and href not in pdf_links:
                if not href.startswith("http"):
                    href = "https://apps.miamibeachfl.gov" + href
                pdf_links.append(href)

        print(f"  Found {len(pdf_links)} potential agenda PDF links")

        # Parse the most recent 2 PDFs only
        parsed = 0
        for link in pdf_links[:4]:
            if parsed >= 2:
                break
            try:
                pdf_cases = parse_agenda_pdf(link)
                if pdf_cases:
                    cases.extend(pdf_cases)
                    parsed += 1
                time.sleep(1)
            except Exception as e:
                print(f"  ✗ Error parsing PDF {link}: {e}")

    except Exception as e:
        print(f"  ✗ Error fetching agenda page: {e}")
        traceback.print_exc()

    print(f"  ✓ City feed: {len(cases)} cases found")
    return cases


def parse_agenda_pdf(url: str) -> list:
    """Download and parse an agenda PDF, extracting case records."""
    try:
        import pdfplumber
        import io

        print(f"  Downloading PDF: {url[:80]}...")
        resp = SESSION.get(url, timeout=30)
        if resp.status_code != 200:
            return []
        if "pdf" not in resp.headers.get("content-type", "").lower() and not url.lower().endswith(".pdf"):
            return []

        cases = []
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

        # Parse blocks — Miami Beach agenda format:
        # Special Master Case# SMB2026-XXXXX
        # Department Violation # BVXXXX
        # Property Address: ...
        # Owner: ...
        # Description: ...
        # Status: ...
        # Fine: $X,XXX.XX
        blocks = re.split(r"(?=Special\s+M(?:aster|agistrate)\s+Case#)", full_text, flags=re.I)

        for block in blocks[1:]:  # Skip preamble
            try:
                case = extract_case_from_block(block)
                if case:
                    cases.append(case)
            except:
                pass

        print(f"    → Extracted {len(cases)} cases from PDF")
        return cases

    except ImportError:
        print("  ✗ pdfplumber not installed")
        return []
    except Exception as e:
        print(f"  ✗ PDF parse error: {e}")
        return []


def extract_case_from_block(block: str) -> dict:
    """Extract structured case data from a text block."""
    def find(pattern, default="—"):
        m = re.search(pattern, block, re.I | re.DOTALL)
        return m.group(1).strip() if m else default

    case_num   = find(r"Case#\s*(\S+)")
    dept_viol  = find(r"(?:Department|Dept\.?)\s+Violation\s*#?\s*(\S+)")
    address    = find(r"Property\s+Address:\s*(.+?)(?:\n|Unit:|Owner:)")
    owner      = find(r"Owner:\s*(.+?)(?:\n|Description:|Status:)")
    desc       = find(r"Description:\s*(.+?)(?:\n|Status:|Fine:)", "")
    status     = find(r"Status:\s*(\S+)")
    fine_str   = find(r"(?:Fine|Balance):\s*\$?([\d,]+\.?\d*)", "0")
    hearing    = find(r"(?:Hearing|Date):\s*([\d/]+)", "—")

    if case_num == "—" or address == "—":
        return None

    return {
        "id":          abs(hash(case_num)) % 999999,
        "property":    address.upper().strip(),
        "caseNum":     case_num,
        "deptViol":    dept_viol,
        "type":        classify_type(dept_viol + " " + desc),
        "status":      status,
        "hearing":     hearing,
        "balance":     parse_dollar(fine_str),
        "dailyFine":   None,
        "owner":       owner.strip(),
        "description": desc.strip()[:200],
        "dateAdded":   datetime.utcnow().strftime("%Y-%m-%d"),
        "contacted":   False,
    }


def classify_type(text: str) -> str:
    t = text.upper()
    if "BVE" in t or "ELECTRIC" in t:   return "Building – Electrical"
    if "BVP" in t or "PLUMB" in t:      return "Building – Plumbing"
    if "BVM" in t or "MECHANIC" in t:   return "Building – Mechanical"
    if "US" in t or "UNSAFE" in t:      return "Unsafe Structures"
    if "EBR" in t or "RECERT" in t:     return "40-Yr Recertification"
    if "BVC" in t or "COMBO" in t:      return "Building – Combo"
    if "PM" in t or "MAINTENANCE" in t: return "Property Maintenance"
    if "CC" in t or "CODE" in t:        return "City Code Violation"
    return "Violation"


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("VioTrack Scraper — Miami Beach SM Portal")
    print(f"Started: {datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    data = load_data()

    # 1. Scrape watched addresses
    print("\n[1/2] Scraping watched addresses...")
    all_my_cases = []
    for addr in data.get("watchAddresses", WATCH_ADDRESSES):
        cases = scrape_address(addr)
        all_my_cases.extend(cases)
        time.sleep(2)  # Be polite

    if all_my_cases:
        # Merge — keep existing cases, update status/balance if case number matches
        existing = {c["caseNum"]: c for c in data.get("myProperties", [])}
        for new_case in all_my_cases:
            cn = new_case["caseNum"]
            if cn in existing:
                # Update mutable fields only
                existing[cn]["status"]  = new_case.get("status", existing[cn]["status"])
                existing[cn]["balance"] = new_case.get("balance", existing[cn]["balance"])
                existing[cn]["hearing"] = new_case.get("hearing", existing[cn]["hearing"])
            else:
                existing[cn] = new_case
        data["myProperties"] = list(existing.values())
        print(f"  ✓ My Properties: {len(data['myProperties'])} total cases")
    else:
        print("  ⚠ No cases scraped — keeping existing data (portal may have changed)")

    # 2. Scrape city feed
    print("\n[2/2] Scraping city agenda feed...")
    city_cases = scrape_city_feed()

    if city_cases:
        # Merge by case number — preserve contacted status
        existing_city = {c["caseNum"]: c for c in data.get("cityFeed", [])}
        for new_case in city_cases:
            cn = new_case["caseNum"]
            if cn not in existing_city:
                existing_city[cn] = new_case  # Only add truly new cases
            else:
                # Update balance/status but preserve contacted flag
                existing_city[cn]["balance"] = new_case.get("balance", existing_city[cn]["balance"])
                existing_city[cn]["status"]  = new_case.get("status", existing_city[cn]["status"])
        data["cityFeed"] = list(existing_city.values())
        print(f"  ✓ City Feed: {len(data['cityFeed'])} total cases")
    else:
        print("  ⚠ No city cases scraped — keeping existing data")

    save_data(data)
    print(f"\nDone. Updated {DATA_FILE}")


if __name__ == "__main__":
    main()

"""
VioTrack Scraper - Rewritten to match actual Miami Beach SM portal structure
"""

import json
import re
import time
import traceback
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup

# ── Config ───────────────────────────────────────────────────────────────────
ADDRESS_INQUIRY_URL = "https://apps.miamibeachfl.gov/SMAddressInquire/"
AGENDA_LIST_URL     = "https://apps.miamibeachfl.gov/energovagenda/Public/"
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
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_address(raw: str):
    parts = raw.strip().upper().split()
    if not parts: return None, None
    number = parts[0]
    name_parts = [p for p in parts[1:] if p not in STREET_TYPES]
    return number, " ".join(name_parts)

def parse_dollar(text: str) -> int:
    cleaned = re.sub(r"[^\d.]", "", text or "")
    try: return int(float(cleaned))
    except: return 0

def classify_type(text: str) -> str:
    t = text.upper()
    if "ELECTRICAL" in t or "BVE" in t:  return "Building – Electrical"
    if "PLUMBING"   in t or "BVP" in t:  return "Building – Plumbing"
    if "MECHANICAL" in t or "BVM" in t:  return "Building – Mechanical"
    if "UNSAFE"     in t:                return "Unsafe Structures"
    if "RECERT"     in t or "EBR" in t:  return "40-Yr Recertification"
    if "COMBO"      in t or "BVC" in t:  return "Building – Combo"
    if "MAINTENANCE" in t:               return "Property Maintenance"
    if "CODE"       in t:                return "City Code Violation"
    if "PARKING"    in t or "PV" in t:   return "Parking Violation"
    if "FIRE"       in t:                return "Fire Violation"
    return text.strip() or "Violation"

def load_data():
    if DATA_FILE.exists(): return json.loads(DATA_FILE.read_text())
    return {"myProperties": [], "cityFeed": [], "watchAddresses": WATCH_ADDRESSES, "lastUpdated": None}

def save_data(data):
    data["lastUpdated"] = datetime.utcnow().isoformat() + "Z"
    DATA_FILE.write_text(json.dumps(data, indent=2))
    print(f"Saved {DATA_FILE}")

# ── Part 1: Address inquiry ───────────────────────────────────────────────────

def scrape_address(raw_address: str) -> list:
    """POST to address inquiry form, parse HTML results table."""
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
            "StreeName":    street_name,   # Portal has typo: StreeName not StreetName
            "ViolationType": "%",          # % = All Violations
        }
        resp2 = SESSION.post(ADDRESS_INQUIRY_URL, data=payload, timeout=15)
        resp2.raise_for_status()
        soup2 = BeautifulSoup(resp2.text, "html.parser")
        cases = []
        table = soup2.find("table")
        if not table:
            print(f"    No results table for {raw_address}")
            return []
        rows = table.find_all("tr")[1:]  # skip header
        for row in rows:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 5: continue
            # Columns: 0=hearing, 1=caseNum, 2=violation#, 3=type, 4=status, 5=name, 6=closed, 7=balance
            case_num  = cells[1] if len(cells) > 1 else "—"
            dept_viol = cells[2] if len(cells) > 2 else "—"
            viol_type = cells[3] if len(cells) > 3 else "—"
            status    = cells[4] if len(cells) > 4 else "—"
            owner     = cells[5] if len(cells) > 5 else "—"
            balance   = parse_dollar(cells[7]) if len(cells) > 7 else 0
            hearing   = cells[0].split()[0]  if cells[0] else "—"
            cases.append({
                "id":         abs(hash(case_num)) % 999999,
                "property":   raw_address,
                "caseNum":    case_num,
                "deptViol":   dept_viol,
                "type":       classify_type(viol_type),
                "status":     status,
                "hearing":    hearing,
                "balance":    balance,
                "dailyFine":  None,
                "owner":      owner,
                "description": viol_type,
                "code":       "",
            })
        print(f"    Found {len(cases)} cases for {raw_address}")
        return cases
    except Exception as e:
        print(f"    Error scraping {raw_address}: {e}")
        traceback.print_exc()
        return []

# ── Part 2: City Feed via Agenda Schedules ────────────────────────────────────

def get_hidden_fields(soup) -> dict:
    fields = {}
    for inp in soup.find_all("input", {"type": "hidden"}):
        name = inp.get("name")
        if name: fields[name] = inp.get("value", "")
    return fields

def scrape_city_feed() -> list:
    """
    1. GET agenda listing page.
    2. POST __doPostBack for each recent BUILDING/CODE hearing.
    3. Parse embedded report HTML directly (no PDFs needed).
    """
    print("\nScraping city agenda feed...")
    cases = []
    try:
        resp = SESSION.get(AGENDA_LIST_URL, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        hidden = get_hidden_fields(soup)

        # Find the agenda GridView table
        grid = soup.find("table", id=re.compile(r"GridView", re.I))
        if not grid:
            # Try any table on the page
            grid = soup.find("table")
        if not grid:
            print("  Could not find agenda table")
            return []

        rows = grid.find_all("tr")[1:]
        print(f"  Found {len(rows)} hearing rows")

        processed = 0
        for i, row in enumerate(rows[:15]):
            cells = row.find_all("td")
            if len(cells) < 3: continue
            hearing_date = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            description  = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            if not any(k in description.upper() for k in ["BUILDING", "BLDG", "CODE", "UNSAFE"]):
                continue
            print(f"  Processing row {i}: {hearing_date} - {description}")
            try:
                hcases = post_and_parse_agenda(soup, hidden, i, hearing_date)
                if hcases:
                    cases.extend(hcases)
                    processed += 1
                time.sleep(2)
            except Exception as e:
                print(f"    Error on row {i}: {e}")
            if processed >= 4:
                break
    except Exception as e:
        print(f"  Error: {e}")
        traceback.print_exc()
    print(f"  City feed total: {len(cases)} cases")
    return cases

def post_and_parse_agenda(original_soup, hidden_fields: dict, row_idx: int, hearing_date: str) -> list:
    """POST __doPostBack to select a hearing row, then parse the AgendaSchedules HTML."""
    payload = dict(hidden_fields)
    payload["__EVENTTARGET"]   = "ctl00$MainContent$GridViewAgendas"
    payload["__EVENTARGUMENT"] = "Select$" + str(row_idx)
    resp = SESSION.post(AGENDA_LIST_URL, data=payload, timeout=25)
    resp.raise_for_status()
    return parse_agenda_html(resp.text, hearing_date)

def parse_agenda_html(html: str, hearing_date: str) -> list:
    """
    The AgendaSchedules response embeds the SSRS report as plain text in the HTML.
    Extract case blocks by splitting on "Special M(aster|agistrate) Case#".
    """
    soup = BeautifulSoup(html, "html.parser")
    # Get all text, using space separator to avoid merging words
    text = soup.get_text(" ")
    text = re.sub(r"\s+", " ", text)
    blocks = re.split(r"(?=Special M(?:aster|agistrate) Case#)", text, flags=re.I)
    cases = []
    for block in blocks[1:]:
        try:
            c = extract_case_from_block(block, hearing_date)
            if c: cases.append(c)
        except Exception:
            pass
    print(f"    Parsed {len(cases)} cases from agenda")
    return cases

def extract_case_from_block(block: str, hearing_date: str) -> dict:
    def find(pattern, default="—"):
        m = re.search(pattern, block, re.I | re.DOTALL)
        return m.group(1).strip() if m else default

    case_num  = find(r"Case#\s*(\S+)")
    dept_viol = find(r"Department Violation\s*#?\s*(\S+)")
    address   = find(r"Property Address:\s*(.+?)(?:\s+(?:Department|Owner|\d+\s+\d+:\d+AM|\d+\s+\d+:\d+PM))")
    owner     = find(r"(?:c/o|Owner|Name):\s*(.+?)(?:\s+(?:AREA|Description|Status|Inspector))", "Unknown")
    desc      = find(r"Description:\s*(.+?)(?:\s+(?:Inspector|Status|Fine|Code|Violation Type))", "")
    status    = find(r"Status:\s*(\w+)")
    fine_raw  = find(r"\$([\d,]+(?:\.\d{2})?)", "0")
    vtype_raw = find(r"Violation Type:\s*(.+?)(?:\s+Code)", "")

    if case_num == "—": return None

    return {
        "id":         abs(hash(case_num)) % 999999,
        "property":   address.upper().strip() if address != "—" else "MIAMI BEACH",
        "caseNum":    case_num,
        "deptViol":   dept_viol,
        "type":       classify_type(dept_viol + " " + vtype_raw),
        "status":     status,
        "hearing":    hearing_date,
        "balance":    parse_dollar(fine_raw),
        "dailyFine":  None,
        "owner":      owner.strip(),
        "description": desc.strip()[:200],
        "dateAdded":  datetime.utcnow().strftime("%Y-%m-%d"),
        "contacted":  False,
    }

# ── Main ──────────────────────────────────────────────────────────────────────

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
                existing[cn]["status"]  = nc.get("status",  existing[cn]["status"])
                existing[cn]["balance"] = nc.get("balance", existing[cn]["balance"])
                existing[cn]["hearing"] = nc.get("hearing", existing[cn]["hearing"])
            else:
                existing[cn] = nc
        data["myProperties"] = list(existing.values())
        print(f"  My Properties: {len(data['myProperties'])} total cases")
    else:
        print("  No new cases — keeping existing data")

    print("\n[2/2] Scraping city agenda feed...")
    city_cases = scrape_city_feed()
    if city_cases:
        existing_city = {c["caseNum"]: c for c in data.get("cityFeed", [])}
        for nc in city_cases:
            cn = nc["caseNum"]
            if cn not in existing_city:
                existing_city[cn] = nc
            else:
                existing_city[cn]["balance"] = nc.get("balance", existing_city[cn]["balance"])
                existing_city[cn]["status"]  = nc.get("status",  existing_city[cn]["status"])
        data["cityFeed"] = list(existing_city.values())
        print(f"  City Feed: {len(data['cityFeed'])} total cases")
    else:
        print("  No city cases — keeping existing data")

    save_data(data)
    print(f"\nDone. Updated {DATA_FILE}")


if __name__ == "__main__":
    main()

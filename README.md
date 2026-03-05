# VioTrack — Miami Beach Violation Intelligence Platform

**Aventura Code Violations** | aventuracodeviolations@gmail.com | (305) 523-9801

## What This Is
A private web app that tracks Miami Beach Special Magistrate violations — for your own properties and as a citywide lead generation feed.

## Repo Structure
```
/
├── index.html              ← The app (open this in a browser or GitHub Pages)
├── scraper.py              ← Python scraper for Miami Beach SM portal
├── data/
│   └── violations.json     ← Auto-updated by scraper every Monday
└── .github/
    └── workflows/
        └── scrape.yml      ← GitHub Actions — runs scraper on schedule
```

## Setup (One Time)

### 1. Enable GitHub Pages
- Go to repo **Settings → Pages**
- Source: **Deploy from branch → main → / (root)**
- Your app will be live at: `https://YOUR-USERNAME.github.io/viotrack`

### 2. Enable GitHub Actions
- Go to repo **Settings → Actions → General**
- Set "Workflow permissions" to **Read and write permissions**
- This allows the scraper to commit updated `violations.json` back to the repo

### 3. Run the Scraper Manually First
- Go to **Actions → Scrape Miami Beach Violations → Run workflow**
- This populates the city feed immediately without waiting for Monday

## How It Works
- Every **Monday at 9am UTC** (5am ET), GitHub Actions runs `scraper.py`
- The scraper hits the Miami Beach SM portal and agenda PDFs
- Results are saved to `data/violations.json`
- The app reads that file automatically on load — no server needed

## Tabs
1. **My Properties** — 411 Washington Ave & 1941 Liberty Ave cases with AI remediation plans
2. **City Feed** — All new Miami Beach violations for outreach
3. **Owner Outreach** — AI drafts outreach emails as Aventura Code Violations
4. **Alerts & Contacts** — Miami Beach dept contact directory

## To Add More Watched Addresses
Edit `data/violations.json` and add to the `watchAddresses` array:
```json
"watchAddresses": [
  "411 WASHINGTON AVE",
  "1941 LIBERTY AVE",
  "YOUR NEW ADDRESS HERE"
]
```

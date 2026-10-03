# Rockdale County Motivated Seller Scraper

Finds distressed property owners in Rockdale County, Georgia every morning,
scores how motivated they are likely to be, and hands you a call list and a
CRM import file.

Built for **Revamp Realty Group / Revamp Home Buyers**. Runs itself on GitHub —
there is nothing to install and nothing to remember to do.

---

## What you get each morning

Around **9:15am Eastern**, without you doing anything:

| | |
|---|---|
| **Dashboard** | A web page listing every lead, sorted by motivation, with the top lead shown as a "call first" card. Works on your phone. |
| **CRM file** | `data/ghl_leads.csv`, formatted for GoHighLevel. One row per owner. |
| **Run summary** | A plain-English report on the Actions tab: how many leads, how many are hot, which sources worked. |

Your dashboard address is:

```
https://terrellkday.github.io/Rockdale-Leads-Scraper/
```

---

## First time through

Two settings have to be switched on in GitHub. If you have already done these, skip ahead.

**1. Let the scraper save its results**
Settings → Actions → General → scroll to **Workflow permissions** →
**Read and write permissions** → Save.

**2. Turn on the dashboard**
Settings → Pages → **Source** → **GitHub Actions**. No save button; it applies instantly.

**Then collect your first batch.** Don't wait until tomorrow:

1. Open the **Actions** tab
2. Click **Daily Rockdale Lead Scrape**
3. Click **Run workflow**
4. Set *lookback days* to **30** — the default of 3 only looks back a few days, and
   30 gives you a real list to start from
5. Click the green **Run workflow** button

It takes a few minutes. When it finishes, open the run and read the summary.

---

## The GSCCCA Premium decision

Three of the four sources are free. The fourth — the Clerk's land records
(deeds, liens, judgments), the equivalent of the DeKalb scraper's main source —
lives in the GSCCCA statewide index, whose free tier is name-search only and
can't list "all new filings." A **GSCCCA Premium account ($29.95/month)**
unlocks the Instrument Type Search the scraper needs.

Without it, the scraper still runs every morning on tax sales + legal notices
and skips the clerk source cleanly. With it, add three repository secrets under
Settings → Secrets and variables → Actions:

```
GSCCCA_PREMIUM=1
GSCCCA_USER=<your gsccca username>
GSCCCA_PASS=<your gsccca password>
```

After subscribing, run the workflow once with `GSCCCA_VERIFY=1` (or
`--gsccca-verify` locally) and read the run summary — it reports exactly which
search controls it found, so any page drift is a ten-minute fix.

---

## Using it day to day

**The dashboard** is the fast path. Filter by minimum score, lead type, city,
whether the owner lives at the property, and how recently the document was
filed. Search matches owner names, addresses, parcel numbers and document
numbers. Click any column header to sort.

Scores are color-coded:

| Score | Meaning |
|---|---|
| 80–100 | Very high motivation — call these first |
| 60–79 | High |
| 40–59 | Moderate |
| below 40 | Lower |

**The CRM file** is the button marked *Download CRM file*, or grab
`data/ghl_leads.csv` from the repository. It imports into GoHighLevel directly.

**Absentee owners** are worth noticing. When the owner's mailing address differs
from the property address, they don't live there — usually a landlord, an heir,
or someone who has already moved on. Those convert better than average, so the
dashboard has a filter just for them. (Mailing addresses are only available
once a parcel source is configured — see "Where the leads come from.")

---

## When something needs you

**A bad day cannot erase a good list.** If every source fails, the scraper keeps
the leads already published and marks them as older rather than overwriting them
with an empty file. Each run's per-source status — working, skipped (not
configured), or not available — is in the Actions run summary and on the
dashboard.

---

## Where the leads come from

| Source | What it provides | Notes |
|---|---|---|
| **GSCCCA** — Premium Instrument Type Search | Deeds, liens, judgments, tax executions | Statewide clerk index; needs the $29.95/mo Premium account |
| **Georgia Public Notice** | Foreclosure advertisements, tax sales, probate notices | Georgia Press Association. The Rockdale Citizen, Rockdale's legal organ, publishes here. Six categories scraped: Foreclosures, Tax Sales, Probate Notices, Sheriff/Marshal Sales, Public Sales/Auctions, Debtors/Creditors |
| **Rockdale Tax Commissioner** | Tax sale property list (linked from the tax-sales page) with parcel IDs, owners, amounts owed | Rockdale posts the current sale's list ~4 weeks before each auction and takes it down afterwards — there is no fixed URL. Between sales the scraper finds no listing and reports zero rows, not a failure. The list layout (FILE # / YEARS / PARCEL / OWNER / OPENING BID) is parsed positionally; a defensive parcel-token fallback harvests Rockdale-style parcel IDs from raw text if the positional parse yields nothing |
| **Parcel enrichment (qPublic)** | Owner of record + mailing address for every parcel | The county's SchneiderCorp qPublic site: each parcel's report page loads as a plain GET (`KeyValue=<parcel>`), no login. Best-effort: parcels that won't load keep the source document's data. Cached on disk; capped at 150 lookups/run (`QPUBLIC_MAX_LOOKUPS`) |

**Why foreclosures come from a newspaper rather than the courthouse.** Georgia
uses *non-judicial* foreclosure. There is usually no court case and no recorded
"notice of foreclosure" to find. Instead the lender must advertise a **Notice of
Sale Under Power** in the county's legal organ for four consecutive weeks before
selling on the courthouse steps the first Tuesday of the month. That newspaper
ad is the real signal — which is why this scraper reads legal notices instead of
only searching deed records the way a Florida-built scraper would.

Because the same ad runs four weeks running. Each republication carries the
same ad code, so the scraper treats the four weekly runs as one document: it
is exported once, and the later weeks are recognized as already seen rather
than re-exported as new leads.

---

## How leads are scored

Every lead starts at **30** and climbs:

| | |
|---|---|
| Each distress type, weighted by seriousness: foreclosure +30; probate, lis pendens, or tax +25; tax lien +20; judgment or mechanic's lien +15; medical or HOA lien +12; other lien +10 | +10–30 |
| Lis pendens **and** foreclosure on the same property | +10 |
| Three or more different kinds of distress | +10 |
| Tax sale scheduled (or past sale, now in its redemption period) | +5 |
| Amount owed over $100,000 | +15 |
| Amount owed over $50,000 | +10 |
| Filed within the lookback window | +5 |
| Property address successfully matched | +5 |
| Absentee owner | +6 |
| Verified parcel match | +3 |
| Likely parcel match | +2 |

Capped at 100. Two adjustments pull scores down: a low-confidence match loses
5 points, a released or cancelled lien drops to 40% of its score, and a notice
of commencement caps at 45 — someone renovating a property is investing in it,
not leaving it.

**Signals stack across documents.** If a judgment is filed Monday, a lis pendens
Wednesday, and a foreclosure ad runs Thursday — all on the same house — that is
**one** motivated seller carrying three signals and a stacked score, not three
separate leads. The scraper groups by parcel ID first, then property address,
then owner name.

The CRM file goes further and collapses to **one row per person**. If the same
owner is distressed on several properties, you get a single contact flagged
*"Owns N distressed properties"* rather than duplicate contacts.

---

## What's in the repository

```
scraper/fetch.py                    the scraper
scraper/requirements.txt            what it needs installed
.github/workflows/scrape.yml        the daily schedule
dashboard/index.html                the web dashboard
dashboard/records.json              what the dashboard reads
data/records.json                   your lead list
data/ghl_leads.csv                  your CRM import file
data/seen_documents.json            which documents have been seen before
data/gsccca_discovery.json          how it learned to drive the GSCCCA search
```

The last two look like junk but must not be deleted. Runners are wiped after
every run, so those files are the scraper's only memory. Without them every lead
looks brand new every day and the site has to be relearned from scratch nightly.

---

## Running it on your own computer

Only needed for debugging. The scheduled run needs none of this.

```bash
git clone https://github.com/terrellkday/Rockdale-Leads-Scraper.git
cd Rockdale-Leads-Scraper

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium

python scraper/fetch.py
```

**Watch it work.** To see the browser instead of running it invisibly:

```bash
HEADLESS=false python scraper/fetch.py
```

A Chromium window opens and you can watch it work. This is the fastest way to
understand a failure.

**Preview the dashboard locally.** Opening `index.html` by double-clicking it
will not work — browsers block local pages from reading files next to them.
Instead:

```bash
cd dashboard
python3 -m http.server 8000
```

Then open `http://localhost:8000`.

---

## Settings you can change

Set these as environment variables, or edit the top of `scraper/fetch.py`.

| Setting | Default | What it does |
|---|---|---|
| `LOOKBACK_DAYS` | `3` | How many days back to search |
| `HEADLESS` | `true` | `false` shows the browser |
| `MAX_RETRIES` | `3` | Attempts before a source is given up on |
| `POLITE_DELAY` | `1.2` | Seconds between page requests |
| `GSCCCA_PREMIUM` | *(unset)* | `1` enables the GSCCCA clerk source (needs `GSCCCA_USER`/`GSCCCA_PASS` secrets) |
| `GSCCCA_VERIFY` | *(unset)* | `1` probes the GSCCCA search controls and exits |
| `PARCEL_LAYER` | *(unset)* | ArcGIS FeatureServer URL for parcel enrichment, if one becomes available |
| `SKIP_SOURCES` | *(none)* | Skip sources: `GSCCCA`, `NOTICES`, `TAX`, `PARCELS` |
| `QPUBLIC_MAX_LOOKUPS` | `150` | Max parcel report pages fetched per run |
| `LOG_LEVEL` | `INFO` | `DEBUG` for much more detail |

To change the daily schedule, edit the `cron` line in
`.github/workflows/scrape.yml`. **GitHub cron runs on UTC time, not Eastern.**
`17 13 * * *` is 9:17am Eastern in summer, 8:17am in winter — GitHub does not
adjust for daylight saving.

---

## Document types

`DOCUMENT_TYPE_MAP` near the top of `fetch.py` sorts raw document names into
twelve lead categories: `LP`, `FC`, `TAX`, `TAXLIEN`, `JUD`, `LIEN`, `MECH`,
`HOA`, `MED`, `PRO`, `NOC`, `RELLP`.

Matching is case-insensitive and works on partial text, so `CLAIM OF LIEN -
MATERIALMAN` matches `MATERIALMAN` and files as a mechanic's lien. Order
matters: specific categories are checked before general ones, so a release of
lis pendens is never mistaken for a lis pendens.

**Anything unrecognized is written to `data/unmapped_doc_types.json`** rather
than silently dropped. After a few weeks, look at that file — it shows the real
document names the scraper is seeing but ignoring. Adding a useful one is a
single line:

```python
"HOA": [
    "HOA LIEN",
    "HOMEOWNERS ASSOCIATION LIEN",
    "PROPERTY OWNERS ASSOCIATION",
    "YOUR NEW TERM HERE",
],
```

---

## Troubleshooting

**No leads at all.** Read the run summary on the Actions tab — it names which
sources failed and why.

**The dashboard shows an error.** You are probably opening the file directly
instead of through the web address. Use the GitHub Pages link at the top of this
file.

**Leads have no mailing address.** Mailing addresses come from the qPublic
parcel reports. If a parcel's report page failed to load (the site sometimes
challenges automated browsers), that record keeps the property address only.
The next run retries it — results are cached, not re-fetched daily.

**The GSCCCA search changed.** Run once with `GSCCCA_VERIFY=1` and read the run
summary — it lists every control the adapter found. If the page changed, the
summary shows what's different.

---

## Data format

`records.json`:

```json
{
  "fetched_at": "2026-10-02T11:47:00+00:00",
  "source": "Rockdale County GA Public Records",
  "date_range": { "start": "2026-09-29", "end": "2026-10-02" },
  "total": 146,
  "with_address": 138,
  "sources_report": { "tax_sales": { "ok": true, "count": 43, "error": "" } },
  "records": [
    {
      "doc_num": "TAX-202610-0690010241",
      "doc_type": "Tax Sale / FiFa (ROCKDALE)",
      "filed": "2026-10-06",
      "cat": "TAX",
      "cat_label": "Tax Delinquent / Tax Sale",
      "owner": "SMITH JOHN A",
      "grantee": "Rockdale County Tax Commissioner",
      "amount": 2681.98,
      "legal": "Rockdale County tax sale 2026-10-06. Delinquent tax years 2023. Opening bid $2,681.98.",
      "parcel_id": "0690010241",
      "prop_address": "1234 SAMPLE ROAD SW",
      "prop_city": "Conyers",
      "prop_state": "GA",
      "prop_zip": "30094",
      "clerk_url": "https://rockdaletaxoffice.org/property-tax-sales",
      "source": "Rockdale County Tax Commissioner (tax sale listing)",
      "foreclosure_sale_date": "2026-10-06",
      "status": "active",
      "flags": ["Tax delinquent", "New this week"],
      "score": 65
    }
  ]
}
```

**CRM file columns:** First Name, Last Name, Mailing Address, Mailing City,
Mailing State, Mailing Zip, Property Address, Property City, Property State,
Property Zip, Lead Type, Document Type, Date Filed, Document Number,
Amount/Debt Owed, Seller Score, Motivated Seller Flags, Source, Public Records URL.

Company names are never split. `ARTHA REALTY LLC` goes into First Name
whole with Last Name blank, rather than being mangled.

**Skip-trace file:** `data/skiptrace_import.csv` (mirrored to
`dashboard/skiptrace_import.csv`) is one row per property — `Address, City,
State, Zip, Tag` — with the Tag naming the lead type, so once a deal closes
you can see what brought it in. Where several distress signals sit on the same
house, the tag names the strongest of them. On mornings with nothing new, both
CSVs are written header-only so yesterday's list never masquerades as today's;
every run also leaves a dated snapshot in `data/exports/` (`ghl_YYYY-MM-DD.csv`,
`skiptrace_YYYY-MM-DD.csv`). If a previously exported lead gains a mailing
address on a later run, it goes out separately in `updated_leads.csv`.

---

## Pushing leads straight to GoHighLevel

Optional. The CSV works without it. To enable, add two repository secrets under
Settings → Secrets and variables → Actions:

```
GHL_API_KEY
GHL_LOCATION_ID
```

Only leads scoring 60 or above are pushed by default (`GHL_MIN_SCORE`).

---

## Limitations — read this part

**Public records are messy.** Names are inconsistent, addresses are abbreviated
differently across sources, and some documents have no address at all. Not every
lead will match a property.

**The clerk source needs the Premium subscription.** Without GSCCCA Premium,
the daily deed/lien flow doesn't run — the scraper works from tax sales and
legal notices only.

**Parcel enrichment is best-effort.** Mailing addresses come from the county's
qPublic report pages, which occasionally challenge automated browsers. A parcel
that won't load keeps its property address; the run retries it next time.

**Legal notices are extracted from prose.** Foreclosure ads are paragraphs of
legal text, not database fields. Borrower names, amounts and sale dates are
pulled with pattern matching and will occasionally be wrong or missing.

**Not every source works every day.** County websites go down. The design
assumption is partial success, not perfection — one failure never stops the rest.

**Verify before you act.** This is a research tool. Everything here is indexed
public record information that can be out of date, superseded, or simply wrong.
Confirm ownership, liens and payoff amounts before making an offer or spending
money. Nothing here is legal or financial advice.

**Use it politely.** The scraper rate-limits itself, retries with backoff, and
does not bypass CAPTCHAs, logins, or paywalls. Please keep it that way — these
are public services, and hammering them is how access gets restricted for
everyone.

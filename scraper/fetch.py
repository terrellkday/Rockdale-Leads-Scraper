#!/usr/bin/env python3
"""
Rockdale County, Georgia -- Motivated Seller Lead Scraper
========================================================

Collects newly recorded / distressed-property public records for Rockdale County GA,
matches them to parcel records for a mailing address where available, consolidates
multiple distress signals onto a single property, scores motivation, and writes
JSON + a GoHighLevel-ready CSV.

SOURCES (all verified reachable as of 2026-10-03)
-------------------------------------------------
1. Clerk of Superior Court land records -- GSCCCA Premium Instrument Type Search
   https://apps.gsccca.org/RealEstatePremium/InstrumentTypeSearch.aspx
   Rockdale County's land records are indexed in the Georgia Superior Court
   Clerks' Cooperative Authority statewide index.
   NEEDS a GSCCCA Premium account ($29.95/mo): set GSCCCA_PREMIUM=1 with
   GSCCCA_USER / GSCCCA_PASS secrets. Without it this source is skipped and
   the other sources still run.

2. Foreclosure / probate / tax-sale legal advertisements
   https://www.georgiapublicnotice.com  (Georgia Press Association, free)
   Searchable by county + category + date range. The Rockdale Citizen
   (Rockdale's official legal organ per O.C.G.A. 9-13-140) publishes into this
   database. Driven with Playwright. Six categories are scraped: Foreclosures,
   Tax Sales, Probate Notices, Sheriff/Marshal Sales, Public Sales/Auctions,
   Debtors/Creditors.

3. Rockdale County Tax Commissioner tax sale listing
   https://rockdaletaxoffice.org/property-tax-sales
   Unlike counties with a fixed listing URL, Rockdale posts the current
   tax-sale property list as a document linked from its tax-sales page
   (published ~4 weeks before each sale; e.g. the October 6, 2026 sale).
   Between sales no listing is posted, and the scraper reports zero rows --
   not a failure. Rockdale's list layout (FILE # | YEARS | PARCEL | OWNER |
   OPENING BID) is parsed positionally with pymupdf; a defensive fallback
   harvests Rockdale-style parcel tokens ("0690010241", "045B010022",
   "C380010164") from the raw text.

4. Parcel enrichment: qPublic (SchneiderCorp), AppID=694
   https://qpublic.schneidercorp.com/Application.aspx?AppID=694&LayerID=11394
       &PageTypeID=4&PageID=4834&KeyValue=<PARCEL>
   Each parcel report loads as a plain GET with no login and carries the owner
   of record plus the owner's mailing address -- the two fields that power the
   absentee-owner flag and the CRM mailing columns. Best-effort (see SOURCE 5
   notes). A licensed supplemental CSV can be dropped at
   data/supplemental.csv (see SUPPLEMENTAL ADDRESS FILE below).

Run:            python scraper/fetch.py
Debug headful:  HEADLESS=false python scraper/fetch.py

Nothing in this file bypasses a CAPTCHA, a login, a paywall, or a robots
restriction. Every source is public, rate limited, and retried politely.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import random
import re
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from bs4 import BeautifulSoup

try:
    from dateutil import parser as dateparser
except ImportError:  # pragma: no cover
    dateparser = None

# Playwright is imported lazily inside the async scrapers so that the parcel /
# static-page half of the program still runs on a machine without browsers.

# =============================================================================
# CONFIGURATION
# =============================================================================

COUNTY = "Rockdale"
STATE = "Georgia"
STATE_ABBR = "GA"

# Three days, not one. The scraper looks back a little further than a day so a
# single failed morning cannot leave a permanent hole in the record, and
# NEW_ONLY below strips the overlap back out so exports still read as "today".
def _int_env(name: str, default: int) -> int:
    """int(os.getenv(...)) that survives garbage input instead of crashing the
    import. A bad value falls back to the default."""
    try:
        return int(str(os.getenv(name, default)).strip())
    except (TypeError, ValueError):
        return default


LOOKBACK_DAYS = _int_env("LOOKBACK_DAYS", 3)

# Keep only documents never seen on a previous run. This is what makes each
# daily export genuinely fresh: filed dates are what the county recorded, but
# what matters for outreach is what you have not already mailed.
NEW_ONLY = os.getenv("NEW_ONLY", "1").strip().lower() not in ("0", "false", "no")

PARCEL_LAYER = os.getenv("PARCEL_LAYER", "").strip()
# Rockdale County has no public parcel API; leave unset to skip enrichment.
PARCEL_API = (PARCEL_LAYER + "/query") if PARCEL_LAYER else ""
LEGAL_NOTICE_SEARCH_URL = "https://www.georgiapublicnotice.com/search.aspx"
# No legal-organ PDF fallback exists for Rockdale County (see stub below).
LEGAL_NOTICE_FALLBACK_URL = ""
TAX_SALE_URL = "https://rockdaletaxoffice.org/property-tax-sales"
# Rockdale posts the current tax-sale property list as a document LINKED FROM
# the tax-sales page above (published ~4 weeks before each sale, taken down
# afterwards) -- there is no fixed listing URL. The scraper scans the page for
# listing links each run; between sales it finds none and reports zero rows
# gracefully instead of erroring.
TAX_SALE_LISTING_URLS = [
    # Kept as the seed list for the link scan: the scan itself runs against
    # TAX_SALE_URL. A direct document URL can be added here if the county ever
    # publishes one at a stable address.
]

# --- runtime knobs -----------------------------------------------------------
HEADLESS = os.getenv("HEADLESS", "true").strip().lower() not in ("false", "0", "no")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "45"))
NAV_TIMEOUT_MS = int(os.getenv("NAV_TIMEOUT_MS", "60000"))
POLITE_DELAY = float(os.getenv("POLITE_DELAY", "1.2"))          # seconds between page hits
PARCEL_PAGE_SIZE = int(os.getenv("PARCEL_PAGE_SIZE", "2000"))    # service MaxRecordCount
PARCEL_CACHE_HOURS = int(os.getenv("PARCEL_CACHE_HOURS", "72"))
MAX_NOTICE_DETAILS = int(os.getenv("MAX_NOTICE_DETAILS", "250"))
SKIP_SOURCES = {s.strip().upper() for s in os.getenv("SKIP_SOURCES", "").split(",") if s.strip()}

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)
CONTACT_NOTE = os.getenv("CONTACT_NOTE", "RevampRealtyGroup-LeadResearch")

# --- paths -------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
DASH_DIR = REPO_ROOT / "dashboard"
CACHE_DIR = DATA_DIR / ".cache"
for _d in (DATA_DIR, DASH_DIR, CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

RECORDS_JSON_PATHS = [DASH_DIR / "records.json", DATA_DIR / "records.json"]
GHL_CSV_PATHS = [DATA_DIR / "ghl_leads.csv", DASH_DIR / "ghl_leads.csv"]
UPDATED_CSV_PATHS = [DATA_DIR / "updated_leads.csv", DASH_DIR / "updated_leads.csv"]
SEEN_STATE_PATH = DATA_DIR / "seen_documents.json"
UNKNOWN_DOCTYPES_PATH = DATA_DIR / "unmapped_doc_types.json"
DISCOVERY_PATH = DATA_DIR / "gsccca_discovery.json"
PARCEL_CACHE_PATH = CACHE_DIR / "parcels.json"
ARCHIVE_PATH = DATA_DIR / "archive.json"

# The dashboard reads a rolling window; the archive keeps everything so an
# older lead can still be pulled back up. Both are committed by the workflow.
DASHBOARD_DAYS = int(os.getenv("DASHBOARD_DAYS", "120"))
ARCHIVE_DAYS = int(os.getenv("ARCHIVE_DAYS", "730"))

# =============================================================================
# LANDMARKWEB SELECTOR CONFIG
# =============================================================================
# GSCCCA's premium search is an ASP.NET application. Every control below is
# probed with a *candidate list* tried in order. After subscribing, run once
# with GSCCCA_VERIFY=1 and the recon dump lands in data/gsccca_discovery.json.

# =============================================================================
# DOCUMENT TYPE MAP
# =============================================================================
# NOTE: these are matched case-insensitively as *substrings* against whatever
# DeKalb's LandmarkWeb actually returns in its document-type column. They are
# Georgia terms, not Florida abbreviations. Anything that fails to map is written
# to data/unmapped_doc_types.json so the list can be grown from real data.

DOCUMENT_TYPE_MAP: Dict[str, List[str]] = {
    "RELLP": [
        "RELEASE OF LIS PENDENS", "RELEASE LIS PENDENS", "CANCELLATION OF LIS PENDENS",
        "CANCEL LIS PENDENS", "DISMISSAL OF LIS PENDENS", "RELLP",
    ],
    "LP": [
        "LIS PENDENS", "NOTICE OF LIS PENDENS", "NOTICE LIS PENDENS",
        "NOTICE OF PENDING ACTION", "PENDING LITIGATION",
    ],
    "FC": [
        # Georgia is a non-judicial foreclosure state. The operative phrase is
        # "sale under power" -- there is no such filing as a "pre-foreclosure",
        # that is an industry term, not one that appears in any record.
        "NOTICE OF SALE UNDER POWER", "NOTICE OF FORECLOSURE SALE UNDER POWER",
        "SALE UNDER POWER", "POWER OF SALE", "FORECLOSURE",
        "DEED UNDER POWER", "DEED UNDER POWER OF SALE", "FORECLOSURE DEED",
        "NOTICE OF FORECLOSURE", "FORECLOSURE SALE", "ATTORNEY IN FACT",
        "NOTICE OF DEFAULT", "ACCELERATION", "NOTICE OF INTENT TO FORECLOSE",
        "CONFIRMATION OF SALE", "REPORT OF SALE",
        # A deed in lieu is the owner handing the property over to avoid the
        # sale. The house itself is gone by then, but the person is a proven
        # distressed owner and often holds others -- worth catching.
        "DEED IN LIEU", "DEED IN LIEU OF FORECLOSURE", "IN LIEU OF FORECLOSURE",
        "VOLUNTARY CONVEYANCE", "SHORT SALE", "LOSS MITIGATION",
        "FORBEARANCE", "NOTICE OF ACCELERATION", "BANKRUPTCY",
        "AUTOMATIC STAY", "RELIEF FROM STAY", "MOTION FOR RELIEF",
    ],
    "TAX": [
        "TAX SALE", "TAX DEED", "TAX FI FA", "TAX FIFA", "TAX FI. FA.",
        "TAX EXECUTION", "TAX COMMISSIONER EXECUTION", "DELINQUENT TAX",
        "TAX LEVY", "LEVY AND SALE", "EXCESS FUNDS", "REDEMPTION",
        "BARMENT OF REDEMPTION", "NOTICE OF FORECLOSURE OF RIGHT TO REDEEM",
        "FORECLOSURE OF RIGHT TO REDEEM", "RIGHT TO REDEEM", "BARMENT",
        "48-4-5", "48-4-45", "48-4-46",
    ],
    "TAXLIEN": [
        "FEDERAL TAX LIEN", "NOTICE OF FEDERAL TAX LIEN", "IRS LIEN", "IRS TAX LIEN",
        "STATE TAX LIEN", "CORPORATE TAX LIEN", "CORP TAX LIEN", "FEDERAL LIEN",
        "GA DEPARTMENT OF REVENUE", "DEPARTMENT OF REVENUE LIEN",
        "STATE TAX EXECUTION", "WITHHOLDING TAX LIEN", "SALES TAX LIEN",
        # GDOR is how DeKalb indexes a Georgia Department of Revenue lien.
        "GDOR LIEN", "GDOR",
        "CERTIFICATE OF FEDERAL TAX RELEASE", "RELEASE OF FEDERAL TAX LIEN",
    ],
    "JUD": [
        # In Georgia a money judgment is recorded on the General Execution
        # Docket as a fi fa (writ of fieri facias). "GED" and "fi fa" are what
        # actually appear in the index, far more often than the word judgment.
        "JUDGMENT", "CERTIFIED JUDGMENT", "DOMESTIC JUDGMENT", "FOREIGN JUDGMENT",
        "DEFAULT JUDGMENT", "CONSENT JUDGMENT", "SUMMARY JUDGMENT",
        # DeKalb's index spells it "FIERA FACIAS" -- their typo, but it is the
        # string that comes back, so it has to be matched as written.
        "FI FA", "FIFA", "FI. FA.", "FIERI FACIAS", "WRIT OF FIERI FACIAS",
        "FIERA FACIAS", "WRIT OF FIERA FACIAS",
        "GENERAL EXECUTION", "GENERAL EXECUTION DOCKET", "GED",
        "WRIT OF EXECUTION", "GARNISHMENT", "ATTACHMENT", "LEVY",
    ],
    "MECH": [
        "MECHANIC LIEN", "MECHANICS LIEN", "MECHANIC'S LIEN",
        "MATERIALMAN", "MATERIALMEN", "CLAIM OF LIEN", "LABORER'S LIEN",
        "LIENS FILES ON REAL ESTATE RECORD", "LIENS FILED ON REAL ESTATE",
        "CONTRACTOR LIEN", "NOTICE OF LIEN RIGHTS", "PRELIMINARY NOTICE",
    ],
    "HOA": [
        "HOA LIEN", "HOMEOWNER", "HOMEOWNERS ASSOCIATION LIEN",
        "CONDOMINIUM ASSOCIATION LIEN", "CONDO LIEN", "ASSOCIATION LIEN",
        "PROPERTY OWNERS ASSOCIATION", "ASSESSMENT LIEN", "DECLARATION OF LIEN",
    ],
    "MED": [
        "MEDICAID LIEN", "MEDICAID", "DEPARTMENT OF COMMUNITY HEALTH",
        "HOSPITAL LIEN", "GOVERNMENT LIEN", "MEDICAL LIEN", "CHILD SUPPORT LIEN",
    ],
    "PRO": [
        # Estate property changes hands through these instruments, and an heir
        # who has just inherited a house is one of the strongest sellers there is.
        "EXECUTOR", "EXECUTRIX", "ADMINISTRATOR", "ADMINISTRATRIX",
        "YEAR'S SUPPORT", "YEARS SUPPORT", "ASSENT TO DEVISE",
        "AFFIDAVIT OF HEIRSHIP", "HEIRSHIP", "ESTATE OF", "LETTERS TESTAMENTARY",
        "LETTERS OF ADMINISTRATION", "PROBATE", "DECEASED", "DEATH CERTIFICATE",
        "PETITION FOR LETTERS", "GUARDIAN", "CONSERVATOR", "TESTAMENTARY",
        # Taken from DeKalb's own index. An heir who has just been handed a
        # house through year's support or an estate deed is about the most
        # motivated seller on any list.
        "ORDER OF YEAR'S SUPPORT", "DEED - FROM ESTATE", "DEED FROM ESTATE",
        "ESTATE DOCUMENTATION", "CERTIFICATE OF DEATH RECORD",
        "DEATH RECORD", "TRUSTEE DEED", "ESTATE",
    ],
    "NOC": [
        "NOTICE OF COMMENCEMENT",
    ],
    "LIEN": [
        "LIEN",  # deliberately last-resort: only reached if nothing above matched
    ],
}

# Order matters. More specific categories must be tested before generic ones.
CATEGORY_ORDER = ["RELLP", "LP", "FC", "TAX", "TAXLIEN", "MECH", "HOA", "MED", "PRO", "NOC", "JUD", "LIEN"]

CAT_LABELS = {
    "LP": "Lis Pendens",
    "FC": "Pre-Foreclosure",
    "TAX": "Tax Delinquent / Tax Sale",
    "JUD": "Judgment",
    "TAXLIEN": "Tax Lien",
    "LIEN": "Lien",
    "MECH": "Mechanic / Materialman Lien",
    "HOA": "HOA / Condo Lien",
    "MED": "Government / Medicaid Lien",
    "PRO": "Probate / Estate",
    "NOC": "Notice of Commencement",
    "RELLP": "Released Lis Pendens",
    "UNK": "Other Recorded Document",
}

CAT_FLAGS = {
    "LP": "Lis pendens",
    "FC": "Pre-foreclosure",
    "TAX": "Tax delinquent",
    "JUD": "Judgment lien",
    "TAXLIEN": "Tax lien",
    "LIEN": "Lien",
    "MECH": "Mechanic lien",
    "HOA": "HOA lien",
    "MED": "Government lien",
    "PRO": "Probate / estate",
}

# Categories that count as "real" distress for the stacking bonus.
DISTRESS_CATEGORIES = {"LP", "FC", "TAX", "JUD", "TAXLIEN", "LIEN", "MECH", "HOA", "MED", "PRO"}

# Documents that cancel out an earlier distress signal.
RELEASE_TERMS = [
    "RELEASE OF LIEN", "RELEASE OF LIS PENDENS", "CANCELLATION", "CANCEL",
    "SATISFACTION", "WITHDRAWAL", "RELEASE OF JUDGMENT", "RELEASE OF FIFA",
    # DeKalb's own wording for a debt that has been cleared. A released lien
    # must not keep scoring as live distress.
    "PARTIAL RELEASE", "BLANKET CANCELLATION", "QUIT CLAIM DEED RELEASING",
    "CERTIFICATE OF FEDERAL TAX RELEASE", "UCC TERMINATION", "TERMINATION",
    "VOID", "LIEN CANCELLATION",
    # DeKalb abbreviates it: "TAX COMM FIFA CANC" means the delinquent tax was
    # paid. Without this the county's own cancellations scored as live distress.
    "FIFA CANC", "FI FA CANC", "TAX COMM FIFA CANC", " CANC",
]

ENTITY_TOKENS = {
    "LLC", "LLLP", "LLP", "LP", "INC", "CORP", "CORPORATION", "COMPANY", "CO",
    "TRUST", "TRUSTEE", "ESTATE", "HOLDINGS", "HOLDING", "PROPERTIES", "PROPERTY",
    "INVESTMENTS", "INVESTMENT", "PARTNERS", "PARTNERSHIP", "LTD", "ASSOCIATION",
    "ASSOC", "BANK", "NA", "FUND", "GROUP", "ENTERPRISES", "VENTURES", "REALTY",
    "HOMES", "CAPITAL", "MANAGEMENT", "SERVICES", "CHURCH", "CITY", "COUNTY",
    "AUTHORITY", "DEPARTMENT", "STATE", "UNITED", "USA", "COMMISSION",
}

NAME_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "MD", "DDS", "ESQ"}

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s  %(levelname)-7s %(name)-14s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("rockdale")
logging.getLogger("urllib3").setLevel(logging.WARNING)

SOURCE_REPORT: Dict[str, Dict[str, Any]] = {}


def record_source_result(name: str, ok: bool = True, count: int = 0,
                         error: str = "", status: str = "") -> None:
    """
    Record how a source fared: status is "ok", "skipped" or "failed".

    A source that ran without raising but returned nothing has NOT worked,
    and reporting it as "working" hides the only failure that matters.
    Anything that yields zero records is recorded as a problem unless it was
    deliberately skipped. An unconfigured optional source (no premium
    account, no parcel layer) is "skipped", not "failed" -- there is nothing
    broken, just nothing to do.
    """
    if not status:
        low = error.lower()
        if "skip" in low or "not configured" in low or "no premium account" in low:
            status = "skipped"
        elif ok and (count > 0 or name == "browser"):
            status = "ok"
        elif ok:
            status = "failed"
            error = error or "ran but found no records"
        else:
            status = "failed"
    SOURCE_REPORT[name] = {"ok": status == "ok", "status": status,
                           "count": count, "error": error[:400]}


# =============================================================================
# GENERIC UTILITIES
# =============================================================================

EASTERN = ZoneInfo("America/New_York")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_et() -> datetime:
    """Wall-clock time in Rockdale. Every date the pipeline stamps or compares
    comes from here -- the Action fires on a UTC cron, so UTC dates roll over
    while it is still yesterday in Georgia."""
    return datetime.now(EASTERN)


def today_et() -> str:
    return now_et().strftime("%Y-%m-%d")


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Scraper-Purpose": CONTACT_NOTE,
    })
    return s


def retry(times: int = MAX_RETRIES, base_delay: float = 1.5, label: str = ""):
    """Decorator: retry a *sync* callable with exponential backoff + jitter."""
    def outer(fn):
        def inner(*args, **kwargs):
            last = None
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 - deliberate catch-all
                    last = exc
                    if attempt == times:
                        break
                    wait = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.6)
                    log.warning("%s attempt %d/%d failed (%s); retrying in %.1fs",
                                label or fn.__name__, attempt, times, exc, wait)
                    time.sleep(wait)
            raise last  # type: ignore[misc]
        return inner
    return outer


async def aretry(coro_fn, *args, times: int = MAX_RETRIES, base_delay: float = 2.0,
                 label: str = "", **kwargs):
    """Retry an *async* callable with exponential backoff + jitter."""
    last: Optional[Exception] = None
    for attempt in range(1, times + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last = exc
            if attempt == times:
                break
            wait = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
            log.warning("%s attempt %d/%d failed (%s); retrying in %.1fs",
                        label or getattr(coro_fn, "__name__", "task"),
                        attempt, times, exc, wait)
            await asyncio.sleep(wait)
    raise last  # type: ignore[misc]


def safe_write_json(path: Path, payload: Any) -> None:
    """Serialize first, then atomically replace. A crash mid-write can't corrupt."""
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(payload, indent=2, ensure_ascii=False, default=str)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(blob, encoding="utf-8")
    os.replace(tmp, path)


def safe_read_json(path: Path, default: Any = None) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not read %s (%s); using default", path, exc)
    return default


def parse_date(value: Any) -> Optional[datetime]:
    """Very forgiving date parser. Returns tz-naive datetime or None."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, (int, float)):
        # ArcGIS returns epoch milliseconds
        try:
            ms = float(value)
            if ms > 1e11:
                ms /= 1000.0
            return datetime.utcfromtimestamp(ms)
        except Exception:  # noqa: BLE001
            return None
    text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"\s+", " ", text)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d-%b-%Y", "%d-%b-%y",
                "%b %d, %Y", "%B %d, %Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:len(fmt) + 4].strip(), fmt)
        except ValueError:
            continue
    if dateparser is not None:
        try:
            return dateparser.parse(text, fuzzy=True, dayfirst=False).replace(tzinfo=None)
        except Exception:  # noqa: BLE001
            pass
    return None


def fmt_date(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d") if dt else ""


MONEY_RE = re.compile(r"\$\s*([\d]{1,3}(?:,\d{3})*(?:\.\d{1,2})?|\d+(?:\.\d{1,2})?)")


# Nothing in a county index is worth more than this. A "$20,260,040,001" is a
# document number that slipped through, not a debt.
MAX_SANE_AMOUNT = 50_000_000.0


def parse_money(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return v if 0 < v <= MAX_SANE_AMOUNT else None
    text = str(value)
    # A bare run of digits with no currency symbol, comma or decimal point is
    # an identifier, not money. Reading document numbers as debts inflated
    # every score by +15 and made the whole ranking meaningless.
    bare = text.strip()
    if bare.isdigit() and len(bare) >= 7:
        return None
    m = MONEY_RE.search(text)
    if not m:
        text2 = re.sub(r"[^\d.]", "", text)
        try:
            v = float(text2)
            return v if 0 < v <= MAX_SANE_AMOUNT else None
        except ValueError:
            return None
    try:
        v = float(m.group(1).replace(",", ""))
        return v if 0 < v <= MAX_SANE_AMOUNT else None
    except ValueError:
        return None


def sha_key(*parts: Any) -> str:
    raw = "||".join(str(p or "") for p in parts)
    return hashlib.sha1(raw.encode("utf-8", "ignore")).hexdigest()[:16]


_DISPLAY_PREFIX_RE = re.compile(r"^\s*(?:nobreak|nowrap|ellipsis)[_\-]", re.I)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = text.replace("\xa0", " ")
    text = re.sub(r"<[^>]+>", " ", text)
    # The portal prefixes cell values with a CSS class name, e.g.
    # "nobreak_WARRANTY DEED". That is presentation, not data.
    text = _DISPLAY_PREFIX_RE.sub("", text)
    # DeKalb types "ORDER OF YEAR`S SUPPORT" with a backtick. Curly quotes turn
    # up too. All of them mean apostrophe, and a mismatch here silently loses
    # an entire lead category.
    text = re.sub(r"[`\u2018\u2019\u02BC\u00B4]", "'", text)
    return re.sub(r"\s+", " ", text).strip()


# =============================================================================
# NAME NORMALIZATION + VARIANT GENERATION
# =============================================================================

def normalize_name(raw: Any) -> str:
    """Uppercase, strip punctuation noise, collapse whitespace. Keeps entity words."""
    text = clean_text(raw).upper()
    if not text:
        return ""
    text = text.replace("&", " AND ")
    text = re.sub(r"\bL\.?\s?L\.?\s?C\.?\b", "LLC", text)
    text = re.sub(r"\bL\.?\s?P\.?\b", "LP", text)
    text = re.sub(r"\bINC\.?\b", "INC", text)
    text = text.replace(".", " ")
    text = text.replace(",", " , ")
    text = re.sub(r"[^\w,\s'\-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s*,\s*", ", ", text)
    return text.strip(" ,")


def is_entity(name: str) -> bool:
    """True when the owner is a company/trust/government rather than a person."""
    tokens = set(re.split(r"[\s,]+", normalize_name(name)))
    return bool(tokens & ENTITY_TOKENS)


def name_tokens(name: str) -> List[str]:
    norm = normalize_name(name)
    toks = [t for t in re.split(r"[\s,]+", norm) if t]
    return [t for t in toks if t not in NAME_SUFFIXES and len(t) > 0]


def name_variants(raw: Any) -> List[str]:
    """
    Produce the lookup strings this name could be indexed under.

    Deeds index people as "LAST FIRST MIDDLE"; the parcel roll may hold
    "LAST, FIRST M" or "FIRST LAST". Entities are never permuted -- taking
    "REVAMP HOME BUYERS LLC" apart would be destructive.
    """
    norm = normalize_name(raw)
    if not norm:
        return []
    out = {norm, norm.replace(",", "").strip()}
    if is_entity(norm):
        return sorted(v for v in out if v)

    toks = name_tokens(norm)
    if not toks:
        return sorted(out)

    if len(toks) == 2:
        a, b = toks
        out |= {f"{a} {b}", f"{b} {a}", f"{a}, {b}", f"{b}, {a}"}
    elif len(toks) == 3:
        # Every ordering of all three tokens, plus every ordering of the tokens
        # that are not bare middle initials. Never emit a fragment that drops a
        # real name part -- "A SMITH" as an index key is an invitation to
        # attach a lien to a stranger.
        from itertools import permutations
        for perm in permutations(toks):
            out.add(" ".join(perm))
            out.add(f"{perm[0]}, {' '.join(perm[1:])}")
        substantial = [t for t in toks if len(t) > 1]
        if len(substantial) == 2:
            x, y = substantial
            out |= {f"{x} {y}", f"{y} {x}", f"{x}, {y}", f"{y}, {x}"}
    elif len(toks) > 3:
        first, last = toks[0], toks[-1]
        out |= {" ".join(toks), f"{last} {first}", f"{first} {last}",
                f"{last}, {first}", f"{' '.join(toks[1:])} {first}"}

    return sorted(v.strip(" ,") for v in out if v.strip(" ,"))


def token_signature(raw: Any) -> str:
    """
    Order-independent key. "SMITH JOHN A" and "JOHN A SMITH" both collapse to
    "A JOHN SMITH", which is what makes cross-source matching work without
    resorting to fuzzy string distance (and its false positives).
    """
    toks = name_tokens(raw)
    if not toks:
        return ""
    if is_entity(raw):
        return " ".join(toks)
    return " ".join(sorted(toks))


def core_signature(raw: Any) -> str:
    """Signature ignoring single-letter middle initials, for LAST+FIRST matching."""
    toks = [t for t in name_tokens(raw) if len(t) > 1]
    if not toks:
        return ""
    if is_entity(raw):
        return " ".join(toks)
    return " ".join(sorted(toks))


def split_person_name(raw: str, order: str = "last-first") -> Tuple[str, str]:
    """
    Best-effort First / Last split for the GHL CSV.
    Entities are returned whole in the first slot with an empty last name.

    order is "last-first" for deed-index style names ("SMITH JOHN") and
    "natural" for legal-notice style names ("John Smith"). Joint owners
    ("Marcia Davis and Kitoshia Eason") are split on and/& and only the
    first person is exported -- one CRM row per person would double-count.
    """
    norm = normalize_name(raw)
    if not norm:
        return "", ""
    if is_entity(norm):
        return clean_text(raw), ""

    # Joint owners: keep the first person only.
    norm = re.split(r"\s+(?:and|&)\s+", norm, maxsplit=1, flags=re.I)[0].strip()
    if not norm:
        return "", ""

    if "," in norm:
        left, _, right = norm.partition(",")
        last = left.strip().title()
        first = " ".join(right.split()).title()
        return first, last

    toks = [t for t in norm.split() if t]
    toks_ns = [t for t in toks if t not in NAME_SUFFIXES]
    if len(toks_ns) == 1:
        return toks_ns[0].title(), ""
    if order == "natural":
        # "John Smith" -> First="John", Last="Smith".
        # "John Michael Smith" -> First="John Michael", Last="Smith".
        return " ".join(toks_ns[:-1]).title(), toks_ns[-1].title()
    if len(toks_ns) == 2:
        # Deed indexes are overwhelmingly LAST FIRST.
        return toks_ns[1].title(), toks_ns[0].title()
    # LAST FIRST MIDDLE -> First="FIRST MIDDLE", Last="LAST"
    return " ".join(toks_ns[1:]).title(), toks_ns[0].title()


# =============================================================================
# ADDRESS NORMALIZATION
# =============================================================================

STREET_ABBR = {
    "STREET": "ST", "ROAD": "RD", "DRIVE": "DR", "AVENUE": "AVE", "LANE": "LN",
    "COURT": "CT", "CIRCLE": "CIR", "BOULEVARD": "BLVD", "PLACE": "PL",
    "TERRACE": "TER", "TRAIL": "TRL", "PARKWAY": "PKWY", "HIGHWAY": "HWY",
    "SQUARE": "SQ", "POINT": "PT", "CROSSING": "XING", "RUN": "RUN", "WAY": "WAY",
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "NORTHEAST": "NE", "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW",
    "APARTMENT": "APT", "SUITE": "STE", "UNIT": "UNIT", "BUILDING": "BLDG",
}

PO_BOX_RE = re.compile(r"\bP\.?\s?O\.?\s?BOX\b", re.I)


DIRECTIONALS = {"N", "S", "E", "W"}


def normalize_address(raw: Any) -> str:
    text = clean_text(raw).upper()
    if not text:
        return ""
    # Drop periods *without* inserting a space, so "N.E." collapses to "NE"
    # rather than splitting into two tokens. Then blank out other punctuation.
    text = text.replace(".", "")
    text = re.sub(r"[^\w\s]", " ", text)
    parts = [STREET_ABBR.get(p, p) for p in text.split()]

    # Re-join directionals that were already space-separated in the source
    # ("PEACHTREE ST N E" -> "PEACHTREE ST NE").
    merged: List[str] = []
    for tok in parts:
        if (merged and tok in DIRECTIONALS and merged[-1] in DIRECTIONALS
                and len(merged[-1]) == 1):
            merged[-1] = merged[-1] + tok
        else:
            merged.append(tok)
    return re.sub(r"\s+", " ", " ".join(merged)).strip()


def street_only(address: Any, city: Any = "", state: Any = "",
                zip_code: Any = "") -> str:
    """
    Return just the street line: number, name, suffix, direction.

    The export has separate City / State / Zip columns, so anything left on the
    end of the street line gets duplicated -- "123 Main St SE Atlanta GA 30324,
    Atlanta, GA, 30324". Only strips a trailing piece it can actually identify,
    so a street genuinely named after a town survives.
    """
    text = clean_text(address)
    if not text:
        return ""
    # Anything after the first comma is city/state/zip by convention.
    if "," in text:
        text = text.split(",", 1)[0]
    text = re.sub(r"\s+\d{5}(?:-\d{4})?\s*$", "", text)          # trailing ZIP
    text = re.sub(r"\s+(?:GA|GEORGIA)\s*$", "", text, flags=re.I)  # trailing state
    st = clean_text(state)
    if st:
        text = re.sub(rf"\s+{re.escape(st)}\s*$", "", text, flags=re.I)
    ct = clean_text(city)
    if ct:
        text = re.sub(rf"\s+{re.escape(ct)}\s*$", "", text, flags=re.I)
    return text.strip(" ,")


def address_key(street: Any, zip_code: Any = "") -> str:
    """Match key: normalized street line, optionally salted with the 5-digit ZIP."""
    s = normalize_address(street)
    if not s:
        return ""
    z = re.sub(r"\D", "", str(zip_code or ""))[:5]
    return f"{s}|{z}" if z else s


def is_po_box(raw: Any) -> bool:
    return bool(PO_BOX_RE.search(clean_text(raw)))


ADDRESS_IN_TEXT_RE = re.compile(
    # Greedy, not lazy. Lazy matching stopped at the first street type it saw,
    # so "2860 Parkway Close" came back as "2860 Parkway" and then failed to
    # match the parcel. Greedy takes the longest run and backtracks.
    # Numbered highways carry the route number AFTER the street type ("3283
    # Hwy 27 Alternate"), so they get their own branch -- the plain list alone
    # would stop at "3283 HWY" and the parcel match would fail.
    r"\b(\d{1,6}[A-Z]?\s+(?:[A-Z0-9'.\-]+\s+){0,5}"
    r"(?:(?:HWY|HIGHWAY)\s+\d{1,4}[A-Z]?"
    r"(?:\s+(?:ALTERNATE|ALT|BUSINESS|BUS|BYPASS|SPUR|CONNECTOR|LOOP))?|"
    r"ST|STREET|RD|ROAD|DR|DRIVE|AVE|AVENUE|LN|LANE|CT|COURT|CIR|CIRCLE|"
    r"BLVD|BOULEVARD|PL|PLACE|TER|TERRACE|TRL|TRAIL|PKWY|PARKWAY|HWY|HIGHWAY|"
    r"WAY|RUN|XING|CROSSING|SQ|SQUARE|PT|POINT|PATH|BEND|RIDGE|CHASE|WALK|"
    r"CV|COVE|GLN|GLEN|LOOP|MNR|MANOR|OVERLOOK|PASS|VIEW|VLG|VILLAGE|"
    # Metro Atlanta subdivisions use a lot of street types the postal
    # abbreviation list leaves out. Missing one truncates the address and
    # the parcel match then fails on a name that was actually complete.
    r"CLOSE|LANDING|LNDG|GATE|MILL|FARM|HOLLOW|HOLW|KNOLL|KNL|SPUR|TRACE|TRCE|"
    r"VALLEY|VLY|VISTA|VIS|DOWNS|GREEN|GRN|PARK|PLACE|STATION|STA|SUMMIT|SMT|"
    r"CREEK|CRK|WOODS|WOOD|SHOALS|FERRY|BRIDGE|BRG|SPRINGS|SPGS|SPRING|"
    r"HEIGHTS|HTS|HILL|HILLS|LAKE|OAKS|PINES|POINTE|RESERVE|RIDGE|RDG|"
    r"CIRCLE|CROSSING|CORNERS|COMMONS|ARBOR|BLUFF|BROOK|CHAPEL|CLUB|"
    r"COURSE|CREST|DALE|FALLS|FOREST|GARDEN|GLADE|GROVE|HARBOR|"
    r"HAVEN|ISLE|JUNCTION|MEADOW|MEWS|ORCHARD|PLAZA|PRESERVE|"
    r"RIVER|SHORE|TRAIL|VILLAS|WALKWAY|WAY)"
    r"(?:\s+(?:NE|NW|SE|SW|N|S|E|W))?)\b",
    re.I,
)


def extract_address_from_text(text: str) -> str:
    """Pull the most plausible street address out of a block of notice prose."""
    if not text:
        return ""
    candidates = ADDRESS_IN_TEXT_RE.findall(clean_text(text).upper())
    if not candidates:
        return ""
    # The longest hit is nearly always the full street line rather than a fragment.
    return max(candidates, key=len).strip()


ZIP_RE = re.compile(r"\b(3\d{4})(?:-\d{4})?\b")


def extract_zip_from_text(text: str) -> str:
    m = ZIP_RE.search(text or "")
    return m.group(1) if m else ""


# =============================================================================
# BROWSER PREFLIGHT / SELF-REPAIR
# =============================================================================
# Installing the `playwright` pip package does NOT install the Chromium binary
# it drives -- that is a separate download. When the binary is missing, every
# browser-backed source dies with "Executable doesn't exist".
#
# Retrying that is pointless: a missing file will not appear on the second
# attempt. So instead the scraper detects that specific failure, installs the
# browser itself, and carries on. If the install cannot happen (no network, no
# disk, locked-down runner), the browser sources are switched off for the run
# and the HTTP-only sources still produce a lead list.

BROWSER_AVAILABLE: Optional[bool] = None   # None = not yet checked
BROWSER_ERROR: str = ""

_MISSING_BROWSER_SIGNS = (
    "executable doesn't exist",
    "executable does not exist",
    "please run the following command",
    "playwright install",
    "looks like playwright was just installed",
    "browsertype.launch",
    "no such file or directory",
)


def _is_missing_browser(exc: Exception) -> bool:
    """Distinguish 'the browser isn't installed' from 'the website is down'."""
    return any(sign in str(exc).lower() for sign in _MISSING_BROWSER_SIGNS)


def _run_install(cmd: List[str], timeout: int = 900) -> Tuple[bool, str]:
    import subprocess
    log.info("  running: %s", " ".join(cmd))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, "the download timed out"
    except Exception as exc:  # noqa: BLE001
        return False, f"could not start the installer: {exc}"
    if proc.returncode == 0:
        return True, ""
    return False, (proc.stderr or proc.stdout or "").strip()


def install_chromium() -> bool:
    """
    Install the Chromium binary. Returns True on success.

    Two attempts, because they fail for different reasons:

      1. `--with-deps` also installs OS libraries through apt. That is the more
         complete install, but apt can fail for reasons that have nothing to do
         with the browser -- one unrelated broken package repository on the
         machine is enough to sink it.
      2. Plain `playwright install chromium` only downloads the browser. On a
         GitHub Actions runner the OS libraries are already present, so this
         very often succeeds after step 1 failed.

    Trying only the first would throw away a working install over an unrelated
    apt problem.
    """
    log.warning("Chromium is not installed. Installing it now -- a one-time "
                "download of roughly 150 MB, usually a couple of minutes.")

    is_root = getattr(os, "geteuid", lambda: 1)() == 0
    attempts: List[Tuple[str, List[str]]] = []
    if is_root:
        attempts.append(("with system libraries",
                         [sys.executable, "-m", "playwright", "install",
                          "--with-deps", "chromium"]))
    attempts.append(("browser only",
                     [sys.executable, "-m", "playwright", "install", "chromium"]))

    for label, cmd in attempts:
        ok, err = _run_install(cmd)
        if ok:
            log.info("  Chromium installed successfully (%s)", label)
            return True
        log.warning("  install attempt (%s) failed", label)
        for line in err.splitlines()[-4:]:
            log.warning("    %s", line[:200])
        if "apt" in err.lower() or "deb" in err.lower():
            log.info("  that failure came from the system package manager, not "
                     "the browser -- retrying with the browser download only")

    log.error("  every install attempt failed")
    if not is_root:
        log.error("  on a personal machine, run this once by hand:")
        log.error("    python -m playwright install --with-deps chromium")
    return False


async def ensure_browser() -> bool:
    """
    Confirm a browser can actually start, repairing the install once if needed.
    The result is cached, so the check costs a second or two per run, not per
    source. Never raises -- callers get a plain True/False.
    """
    global BROWSER_AVAILABLE, BROWSER_ERROR
    if BROWSER_AVAILABLE is not None:
        return BROWSER_AVAILABLE

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        BROWSER_ERROR = ("the playwright package is not installed "
                         "(pip install -r scraper/requirements.txt)")
        log.error("Browser sources disabled: %s", BROWSER_ERROR)
        BROWSER_AVAILABLE = False
        record_source_result("browser", False, 0, BROWSER_ERROR)
        return False

    for attempt in (1, 2):
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
                version = browser.version
                await browser.close()
            log.info("Browser ready: Chromium %s", version)
            BROWSER_AVAILABLE = True
            record_source_result("browser", True, 0)
            return True
        except Exception as exc:  # noqa: BLE001
            if attempt == 1 and _is_missing_browser(exc):
                if install_chromium():
                    continue          # installed -- try launching once more
                BROWSER_ERROR = "Chromium could not be installed"
            else:
                BROWSER_ERROR = f"Chromium would not start: {str(exc)[:200]}"
            break

    log.error("Browser sources disabled: %s", BROWSER_ERROR)
    log.warning("The run continues without them. Parcel data and any source "
                "reachable over plain HTTP still work, so you will still get a "
                "lead file -- it will just be missing the clerk portal and the "
                "legal-notice site.")
    BROWSER_AVAILABLE = False
    record_source_result("browser", False, 0, BROWSER_ERROR)
    return False


# =============================================================================
# SOURCE 1: DEKALB ARCGIS TAX PARCELS
# =============================================================================
# Field names below were read directly off the live layer definition at
# (DeKalb used https://dcgis.dekalbcountyga.gov/hosted/rest/services/Tax_Parcels/FeatureServer/0)
# The layer reports MaxRecordCount=2000 and supportsPagination=true.

PARCEL_FIELDS = [
    "PARCELID", "LOWPARCELID",
    "OWNERNME1", "OWNERNME2",
    "SITEADDRESS", "ADDRESS_NUMBER", "FULL_STREET_NAME", "UNIT_TYPE", "UNIT_NO",
    "CITY", "STATE", "ZIP",
    "PSTLADDRESS", "PSTLCITY", "PSTLSTATE", "PSTLZIP5", "PSTLZIP4",
    "PRPRTYDSCRP", "CLASSCD", "CLASSDSCRP", "USECD", "USEDSCRP",
    "TOTAPR1", "LASTUPDATE",
]


class ParcelIndex:
    """
    Downloads the full parcel roll once per run (cached on disk between
    runs) and builds the lookup tables used for address enrichment.

    Lookups, in descending order of trustworthiness:
      by_parcel_id   -- exact, authoritative
      by_address     -- normalized site address (+ZIP)
      by_name_exact  -- normalized owner string as printed on the roll
      by_signature   -- order-independent token signature
      by_core        -- signature with middle initials dropped
    """

    def __init__(self) -> None:
        self.parcels: List[Dict[str, Any]] = []
        self.by_parcel_id: Dict[str, Dict[str, Any]] = {}
        self.by_address: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.by_name_exact: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.by_signature: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.by_core: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.loaded_from_cache = False

    # ---------------------------------------------------------------- fetching
    @staticmethod
    @retry(label="arcgis-count")
    def _fetch_count(session: requests.Session) -> int:
        resp = session.get(
            PARCEL_API,
            params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"ArcGIS error: {data['error']}")
        return int(data.get("count", 0))

    @staticmethod
    @retry(label="arcgis-page")
    def _fetch_page(session: requests.Session, offset: int, size: int) -> List[Dict[str, Any]]:
        resp = session.get(
            PARCEL_API,
            params={
                "where": "1=1",
                "outFields": ",".join(PARCEL_FIELDS),
                "returnGeometry": "false",
                "resultOffset": offset,
                "resultRecordCount": size,
                "orderByFields": "OBJECTID ASC",
                "f": "json",
            },
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"ArcGIS error at offset {offset}: {data['error']}")
        return [f.get("attributes", {}) for f in data.get("features", [])]

    def load(self, session: requests.Session) -> None:
        if not PARCEL_LAYER:
            log.info("No parcel source configured (PARCEL_LAYER unset); "
                     "skipping parcel enrichment")
            record_source_result("parcels", True, 0, "no parcel source",
                                 status="skipped")
            return
        cached = self._load_cache()
        if cached is not None:
            self.parcels = cached
            self.loaded_from_cache = True
            log.info("Parcel cache hit: %d parcels (skipping download)", len(self.parcels))
            self._build_indexes()
            record_source_result("arcgis_parcels", True, len(self.parcels))
            return

        try:
            total = self._fetch_count(session)
            log.info("ArcGIS reports %s parcels; downloading attributes only", f"{total:,}")
        except Exception as exc:  # noqa: BLE001
            log.error("Could not read parcel count: %s", exc)
            total = 0

        rows: List[Dict[str, Any]] = []
        offset = 0
        consecutive_failures = 0
        while True:
            try:
                page = self._fetch_page(session, offset, PARCEL_PAGE_SIZE)
                consecutive_failures = 0
            except Exception as exc:  # noqa: BLE001
                consecutive_failures += 1
                log.error("Parcel page at offset %d failed permanently: %s", offset, exc)
                if consecutive_failures >= 2:
                    break
                offset += PARCEL_PAGE_SIZE
                continue

            if not page:
                break
            rows.extend(page)
            offset += PARCEL_PAGE_SIZE
            if offset % 20000 == 0:
                log.info("  ... %s parcels downloaded", f"{len(rows):,}")
            if total and offset >= total + PARCEL_PAGE_SIZE:
                break
            if offset > 1_500_000:  # runaway guard
                log.warning("Parcel pagination guard tripped; stopping")
                break
            time.sleep(0.15)

        self.parcels = rows
        log.info("Parcel download complete: %s records", f"{len(rows):,}")
        if rows:
            self._save_cache(rows)
            record_source_result("arcgis_parcels", True, len(rows))
        else:
            record_source_result("arcgis_parcels", False, 0, "no parcels returned")
        self._build_indexes()

    # ------------------------------------------------------------------- cache
    def _load_cache(self) -> Optional[List[Dict[str, Any]]]:
        if os.getenv("PARCEL_CACHE", "1") == "0":
            return None
        blob = safe_read_json(PARCEL_CACHE_PATH)
        if not isinstance(blob, dict):
            return None
        try:
            fetched = datetime.fromisoformat(blob.get("fetched_at", ""))
        except Exception:  # noqa: BLE001
            return None
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        if utcnow() - fetched > timedelta(hours=PARCEL_CACHE_HOURS):
            return None
        rows = blob.get("parcels")
        return rows if isinstance(rows, list) and rows else None

    def _save_cache(self, rows: List[Dict[str, Any]]) -> None:
        try:
            safe_write_json(PARCEL_CACHE_PATH,
                            {"fetched_at": utcnow().isoformat(), "parcels": rows})
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not write parcel cache: %s", exc)

    # ----------------------------------------------------------------- indexes
    @staticmethod
    def site_address(p: Dict[str, Any]) -> str:
        site = clean_text(p.get("SITEADDRESS"))
        if site and re.search(r"\d", site):
            return site
        num = clean_text(p.get("ADDRESS_NUMBER"))
        street = clean_text(p.get("FULL_STREET_NAME"))
        if not (num or street):
            return ""
        built = " ".join(x for x in (num, street) if x)
        utype = clean_text(p.get("UNIT_TYPE"))
        uno = clean_text(p.get("UNIT_NO"))
        if uno:
            built = f"{built} {utype or 'UNIT'} {uno}".strip()
        return built.strip()

    def _build_indexes(self) -> None:
        for p in self.parcels:
            try:
                pid = clean_text(p.get("PARCELID"))
                low = clean_text(p.get("LOWPARCELID"))
                for key in {normalize_parcel_id(pid), normalize_parcel_id(low)}:
                    if key and key not in self.by_parcel_id:
                        self.by_parcel_id[key] = p

                site = self.site_address(p)
                if site:
                    ak = address_key(site, p.get("ZIP"))
                    if ak:
                        self.by_address[ak].append(p)
                    bare = normalize_address(site)
                    if bare and bare != ak:
                        self.by_address[bare].append(p)

                for owner_field in ("OWNERNME1", "OWNERNME2"):
                    owner = clean_text(p.get(owner_field))
                    if not owner:
                        continue
                    self.by_name_exact[normalize_name(owner)].append(p)
                    for variant in name_variants(owner):
                        self.by_name_exact[variant].append(p)
                    sig = token_signature(owner)
                    if sig:
                        self.by_signature[sig].append(p)
                    core = core_signature(owner)
                    if core and core != sig:
                        self.by_core[core].append(p)
            except Exception as exc:  # noqa: BLE001
                log.debug("Skipping malformed parcel row: %s", exc)

        log.info("Parcel indexes built: %s parcel-ids, %s addresses, %s owner keys",
                 f"{len(self.by_parcel_id):,}", f"{len(self.by_address):,}",
                 f"{len(self.by_name_exact):,}")

    # ---------------------------------------------------------------- matching
    def match(self, parcel_id: str = "", prop_address: str = "",
              owner: str = "", legal: str = "") -> Tuple[Optional[Dict[str, Any]], float, str]:
        """
        Staged match. Returns (parcel, confidence, method).

        Deliberately conservative: an owner key that resolves to more than
        AMBIGUITY_LIMIT parcels is treated as no match rather than guessed,
        because attaching a foreclosure to the wrong house is worse than
        leaving the address blank.
        """
        AMBIGUITY_LIMIT = 3

        # 1. Parcel ID -- authoritative
        pk = normalize_parcel_id(parcel_id)
        if pk and pk in self.by_parcel_id:
            return self.by_parcel_id[pk], 1.0, "parcel_id"

        # 2. Property address printed on the document
        if prop_address:
            for key in (address_key(prop_address), normalize_address(prop_address)):
                if not key:
                    continue
                hits = self.by_address.get(key) or []
                if len(hits) == 1:
                    return hits[0], 0.92, "address"
                if 1 < len(hits) <= AMBIGUITY_LIMIT:
                    return hits[0], 0.72, "address_multi"

        # 3. Exact normalized owner name
        if owner:
            norm = normalize_name(owner)
            hits = self.by_name_exact.get(norm) or []
            hits = _dedupe_parcels(hits)
            if len(hits) == 1:
                return hits[0], 0.85, "owner_exact"
            if 1 < len(hits) <= AMBIGUITY_LIMIT:
                return hits[0], 0.6, "owner_exact_multi"

            # 4. Alternate name orderings / token signature
            sig = token_signature(owner)
            hits = _dedupe_parcels(self.by_signature.get(sig) or [])
            if len(hits) == 1:
                return hits[0], 0.75, "owner_signature"
            if 1 < len(hits) <= AMBIGUITY_LIMIT:
                return hits[0], 0.55, "owner_signature_multi"

            core = core_signature(owner)
            hits = _dedupe_parcels(self.by_core.get(core) or [])
            if len(hits) == 1:
                return hits[0], 0.65, "owner_core"

        # 5. Legal description clue: parcel id embedded in the legal text
        if legal:
            for cand in PARCEL_ID_IN_TEXT_RE.findall(legal.upper()):
                key = normalize_parcel_id(cand)
                if key and key in self.by_parcel_id:
                    return self.by_parcel_id[key], 0.8, "legal_parcel_id"

        return None, 0.0, "none"


def _dedupe_parcels(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen, out = set(), []
    for r in rows:
        pid = clean_text(r.get("PARCELID"))
        if pid and pid in seen:
            continue
        seen.add(pid)
        out.append(r)
    return out


# DeKalb parcel IDs look like "15 126 06 011" or "15-126-06-011".
# Rockdale parcels are compact and alphanumeric: "0690010241", "045B010022",
# "041001022B", "093A01079A", "C380010164". Two shapes: 9-10 bare digits, or
# the letter-bearing form (optional leading letter, three digits, optional
# interior letter, three digits, two to four digits, optional trailing
# letter). The bare-digit form is matched at exactly 9-10 digits so an
# 8-digit YYYYMMDD date cannot match; the letter-bearing form must contain
# at least one letter. A match here only ever attaches a parcel id to an
# existing lead, never creates one.
# The DeKalb-era four-group numeric and Coweta-era letter-prefix patterns are
# kept for regression (ParcelIndex also matches on normalized digits).
PARCEL_ID_IN_TEXT_RE = re.compile(
    r"\b(\d{9,10}"
    r"|(?=[A-Z0-9]*[A-Z])[A-Z]?\d{3}[A-Z]?\d{3}\d{2,4}[A-Z]?"
    r"|[A-Z]\d{2}\s+\d{2,4}"
    r"|\d{2}[\s\-]\d{3}[\s\-]\d{2}[\s\-]\d{3}"
    r"|\d{2,3}[\s\-]\d{3,4}[\s\-]\d{2,3})\b")


def normalize_parcel_id(raw: Any) -> str:
    text = clean_text(raw).upper()
    if not text:
        return ""
    return re.sub(r"[^0-9A-Z]", "", text)


# =============================================================================
# =============================================================================
# SOURCE 2: GSCCCA (Clerk of Superior Court official records)
# =============================================================================
# Rockdale County's land records live in the Georgia Superior Court Clerks'
# Cooperative Authority statewide index (https://search.gsccca.org) -- there is
# no county-run clerk portal. The free tier is name-search only and cannot
# enumerate "all new filings in a date range", so this source needs a GSCCCA
# Premium account ($29.95/mo) with the Instrument Type Search at
# https://apps.gsccca.org/RealEstatePremium/InstrumentTypeSearch.aspx
# (search by county + instrument type + date range).
#
# Gate: set GSCCCA_PREMIUM=1 and provide GSCCCA_USER / GSCCCA_PASS as repo
# secrets. Without them this source is skipped cleanly; the tax-sale and legal
# notice sources still run.
#
# STATUS (2026-10-02): the Premium search page has not been exercised live.
# After subscribing, run the workflow once with GSCCCA_VERIFY=1 and read the
# run summary -- the adapter logs exactly which controls it found, so any
# selector drift is a ten-minute fix, not a mystery.

UNMAPPED_DOC_TYPES: set = set()

# Strings LandmarkWeb renders as page furniture. Without this filter the grid
# scrape happily returns column headers and help text as if they were records.
JUNK_ROW_MARKERS = [
    "GRANTEE SUB BLOCK LOT", "GRANTOR GRANTEE", "NO RECORDS", "NO RESULTS",
    "LOADING", "SEARCH RESULTS", "PLEASE WAIT", "ROWS PER PAGE",
    "SHOWING", "PREVIOUS NEXT", "DISCLAIMER", "COPYRIGHT",
]


def categorize(doc_type: str) -> Tuple[str, bool]:
    """Map a raw document-type string to (category, is_release)."""
    text = clean_text(doc_type).upper()
    if not text:
        return "UNK", False

    is_release = any(term in text for term in RELEASE_TERMS)

    # The most specific phrase wins, not the first category checked. Otherwise
    # "NOTICE OF FORECLOSURE OF RIGHT TO REDEEM" -- a tax-deed barment -- gets
    # filed as a mortgage foreclosure because the word FORECLOSURE appears in
    # it. Ties fall back to CATEGORY_ORDER.
    best_cat, best_len, best_rank = None, 0, 999
    for rank, cat in enumerate(CATEGORY_ORDER):
        for alias in DOCUMENT_TYPE_MAP.get(cat, []):
            if re.search(r"\b" + re.escape(alias) + r"\b", text):
                n = len(alias)
                if n > best_len or (n == best_len and rank < best_rank):
                    best_cat, best_len, best_rank = cat, n, rank
    if best_cat:
        return best_cat, is_release

    UNMAPPED_DOC_TYPES.add(text[:120])
    return "UNK", is_release


def looks_like_record(row: Dict[str, Any]) -> bool:
    """Reject page chrome that survived the DOM scrape."""
    blob = " ".join(str(v) for v in row.values() if v).upper()
    if len(blob) < 8:
        return False
    for marker in JUNK_ROW_MARKERS:
        if marker in blob and len(blob) < 160:
            return False
    # A real index row nearly always carries a date or a document number.
    return bool(row.get("doc_num") or row.get("filed"))



GSCCCA_PREMIUM = os.getenv("GSCCCA_PREMIUM", "").strip().lower() in ("1", "true", "yes")
GSCCCA_VERIFY = os.getenv("GSCCCA_VERIFY", "").strip().lower() in ("1", "true", "yes")
GSCCCA_USER = os.getenv("GSCCCA_USER", "")
GSCCCA_PASS = os.getenv("GSCCCA_PASS", "")
GSCCCA_LOGIN_URL = ("https://apps.gsccca.org/login.asp"
                    "?Redirect=%2fRealEstatePremium%2fInstrumentTypeSearch.aspx")
GSCCCA_SEARCH_URL = "https://apps.gsccca.org/RealEstatePremium/InstrumentTypeSearch.aspx"
MAX_GSCCCA_PAGES = int(os.getenv("MAX_GSCCCA_PAGES", "40"))

# Instrument types worth pulling (GSCCCA's own codes, from the public index).
GSCCCA_WATCH_INSTRUMENTS = [
    ("28", "DEED - FORECLOSURE"),
    ("46", "SHERIFF'S DEED"),
    ("47", "TAX SALE DEED"),
    ("50", "TRUSTEE'S DEED"),
    ("33", "LIEN"),
    ("35", "MATERIALMANS LIEN"),
    ("45", "SECURITY DEED"),
    ("40", "QUIT CLAIM DEED"),
]


class GSCCCAScraper:
    """Playwright driver for the GSCCCA Premium Instrument Type Search.

    Defensive by design: the page has not been exercised live, so every
    control is probed (several selector strategies each) and whatever is
    found is logged. If the page structure differs, the logs say exactly
    what was seen instead of failing cryptically.
    """

    def __init__(self, start: datetime, end: datetime) -> None:
        self.start = start
        self.end = end
        self.notes: List[str] = []
        self.probe: Dict[str, Any] = {}

    async def _probe_controls(self, page) -> Dict[str, bool]:
        """Find the county / instrument / date controls; log what exists."""
        found: Dict[str, bool] = {}
        probes = {
            "county_select": ["select[name*='county' i]", "select[id*='county' i]",
                              "select[name*='County']"],
            "instr_select": ["select[name*='instr' i]", "select[id*='instr' i]",
                             "select[name*='Instrument']", "select[name*='type' i]"],
            "date_from": ["input[name*='from' i]", "input[id*='from' i]",
                          "input[name*='StartDate' i]", "input[name*='begin' i]"],
            "date_to": ["input[name*='to' i]:not([name*='photo' i])",
                        "input[id*='To' i]", "input[name*='EndDate' i]"],
            "search_btn": ["input[type='submit']", "button[type='submit']",
                           "input[value*='search' i]", "button:has-text('Search')"],
        }
        for key, selectors in probes.items():
            hit = None
            for sel in selectors:
                try:
                    loc = page.locator(sel).first
                    if await loc.count() > 0 and await loc.is_visible():
                        hit = sel
                        break
                except Exception:  # noqa: BLE001
                    continue
            found[key] = hit
            self.probe[key] = hit or "NOT FOUND"
        log.info("GSCCCA controls: %s", json.dumps(self.probe))
        return found

    async def _login(self, page) -> bool:
        try:
            await page.goto(GSCCCA_LOGIN_URL, wait_until="domcontentloaded",
                            timeout=NAV_TIMEOUT_MS)
            await page.wait_for_timeout(1500)
            user_sel = None
            for sel in ["input[name='username' i]", "input[name='user' i]",
                        "input[id='username' i]", "input[type='text']"]:
                try:
                    if await page.locator(sel).first.is_visible():
                        user_sel = sel
                        break
                except Exception:  # noqa: BLE001
                    continue
            if not user_sel:
                self.notes.append("login page: no username field found")
                return False
            await page.locator(user_sel).first.fill(GSCCCA_USER)
            pass_sel = None
            for sel in ["input[type='password']"]:
                try:
                    if await page.locator(sel).first.is_visible():
                        pass_sel = sel
                        break
                except Exception:  # noqa: BLE001
                    continue
            if not pass_sel:
                self.notes.append("login page: no password field found")
                return False
            await page.locator(pass_sel).first.fill(GSCCCA_PASS)
            await page.locator("input[type='submit'], button[type='submit']").first.click()
            await page.wait_for_timeout(2500)
            # Logged in if we land on the premium search page or the login
            # form is gone.
            url = page.url.lower()
            if "instrumenttypesearch" in url or "login.asp" not in url:
                log.info("GSCCCA login accepted")
                return True
            self.notes.append("login submitted but still on the login page")
            return False
        except Exception as exc:  # noqa: BLE001
            self.notes.append(f"login failed: {exc}")
            return False

    async def _select_option_like(self, page, select_sel: str, want: str) -> bool:
        """Pick a dropdown option whose text/value contains `want`."""
        try:
            return await page.evaluate(
                """([sel, want]) => {
                    const el = document.querySelector(sel);
                    if (!el) return false;
                    const w = want.toUpperCase();
                    for (const o of el.options) {
                        const t = ((o.text || '') + ' ' + (o.value || '')).toUpperCase();
                        if (t.includes(w)) { el.value = o.value;
                            el.dispatchEvent(new Event('change', {bubbles: true}));
                            return true; }
                    }
                    return false;
                }""", [select_sel, want])
        except Exception:  # noqa: BLE001
            return False

    async def _run_one_instrument(self, page, code: str, label: str) -> List[Dict[str, Any]]:
        """Search one instrument type over the date window; return raw rows."""
        rows: List[Dict[str, Any]] = []
        found = self.probe
        try:
            if found.get("county_select"):
                await self._select_option_like(page, found["county_select"], "ROCKDALE")
                await page.wait_for_timeout(800)
            if found.get("instr_select"):
                # Try the numeric code first, then the label.
                if not await self._select_option_like(page, found["instr_select"], code):
                    await self._select_option_like(page, found["instr_select"], label)
                await page.wait_for_timeout(800)
            if found.get("date_from"):
                await page.locator(found["date_from"]).first.fill(
                    self.start.strftime("%m/%d/%Y"))
            if found.get("date_to"):
                await page.locator(found["date_to"]).first.fill(
                    self.end.strftime("%m/%d/%Y"))
            if found.get("search_btn"):
                await page.locator(found["search_btn"]).first.click()
            else:
                self.notes.append(f"{label}: no search button found")
                return rows
            await page.wait_for_timeout(3000)

            for _ in range(MAX_GSCCCA_PAGES):
                page_rows = await page.evaluate(
                    """() => {
                        const out = [];
                        for (const tr of document.querySelectorAll('table tr')) {
                            const cells = Array.from(tr.querySelectorAll('td'))
                                .map(td => (td.innerText || '').trim())
                                .filter(t => t);
                            if (cells.length >= 3) out.push(cells);
                        }
                        return out;
                    }""")
                for cells in page_rows:
                    rows.append({"_cells": cells, "_instr": label})
                # Next-page control, if any.
                nxt = None
                for sel in ["a:has-text('Next')", "input[value='Next' i]",
                            "a:has-text('>')"]:
                    try:
                        loc = page.locator(sel).first
                        if await loc.count() > 0 and await loc.is_visible():
                            nxt = loc
                            break
                    except Exception:  # noqa: BLE001
                        continue
                if nxt is None:
                    break
                await nxt.click()
                await page.wait_for_timeout(2000)
        except Exception as exc:  # noqa: BLE001
            self.notes.append(f"{label}: {exc}")
        return rows

    def _map_row(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        cells = raw.get("_cells") or []
        instr = raw.get("_instr", "")
        if len(cells) < 3:
            return None
        # Best-effort mapping: GSCCCA result grids carry some ordering of
        # instrument / book-page / filing date / parties / property location.
        # Dates and book/page patterns are recognizable; the rest is positional.
        text = " | ".join(cells)
        filed = None
        for cell in cells:
            dt = parse_date(cell)
            if dt:
                filed = dt
                break
        book_page = ""
        for cell in cells:
            if re.search(r"\b\d{3,6}\s*[-/]\s*\d{1,6}\b", cell):
                book_page = clean_text(cell)
                break
        parties = [c for c in cells
                   if c != book_page and not parse_date(c)]
        grantor = clean_text(parties[0]) if parties else ""
        grantee = clean_text(parties[1]) if len(parties) > 1 else ""
        location = clean_text(parties[2]) if len(parties) > 2 else ""
        cat, is_release = categorize(instr)
        return {
            "doc_num": book_page or f"GSCCCA-{sha_key(text)[:10]}",
            "doc_type": instr,
            "filed": fmt_date(filed),
            "cat": cat,
            "cat_label": CAT_LABELS.get(cat, cat),
            "owner": grantee or grantor,
            "grantor": grantor,
            "grantee": grantee,
            "prop_address": location,
            "clerk_url": GSCCCA_SEARCH_URL,
            "source": "GSCCCA (Rockdale County)",
            "status": "active",
            "is_release": is_release,
            "_gsccca_raw": text[:400],
        }

    async def run(self) -> List[Dict[str, Any]]:
        from playwright.async_api import async_playwright  # noqa: PLC0415
        out: List[Dict[str, Any]] = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=HEADLESS)
            try:
                page = await browser.new_page(user_agent=USER_AGENT)
                if not await self._login(page):
                    log.error("GSCCCA login failed: %s", "; ".join(self.notes))
                    return []
                await page.goto(GSCCCA_SEARCH_URL, wait_until="domcontentloaded",
                                timeout=NAV_TIMEOUT_MS)
                await page.wait_for_timeout(1500)
                found = await self._probe_controls(page)
                if GSCCCA_VERIFY:
                    log.info("GSCCCA_VERIFY=1: controls only, no searches run")
                    return []
                if not found.get("search_btn"):
                    self.notes.append("search form not recognized; aborting")
                    log.error("GSCCCA: %s", "; ".join(self.notes))
                    return []
                for code, label in GSCCCA_WATCH_INSTRUMENTS:
                    raw_rows = await self._run_one_instrument(page, code, label)
                    log.info("GSCCCA %s: %d raw rows", label, len(raw_rows))
                    for raw in raw_rows:
                        rec = self._map_row(raw)
                        if rec:
                            out.append(rec)
                    await page.wait_for_timeout(int(POLITE_DELAY * 1000))
            finally:
                await browser.close()
        if self.notes:
            log.info("GSCCCA notes: %s", "; ".join(self.notes[:8]))
        return out


async def scrape_gsccca(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    if "GSCCCA" in SKIP_SOURCES:
        log.info("Skipping GSCCCA (SKIP_SOURCES)")
        record_source_result("gsccca", True, 0, "skipped")
        return []
    if not GSCCCA_PREMIUM:
        log.info("GSCCCA source skipped: needs a Premium account "
                 "(GSCCCA_PREMIUM=1 with GSCCCA_USER/GSCCCA_PASS secrets)")
        record_source_result("gsccca", True, 0, "no premium account")
        return []
    if not (GSCCCA_USER and GSCCCA_PASS):
        log.warning("GSCCCA_PREMIUM=1 but GSCCCA_USER/GSCCCA_PASS are not set")
        record_source_result("gsccca", False, 0, "missing credentials")
        return []
    if not await ensure_browser():
        record_source_result("gsccca", False, 0, BROWSER_ERROR)
        return []
    scraper = GSCCCAScraper(start, end)
    try:
        rows = await aretry(scraper.run, times=MAX_RETRIES, label="gsccca")
        record_source_result("gsccca", True, len(rows))
        return rows
    except Exception as exc:  # noqa: BLE001
        log.error("GSCCCA source failed after retries: %s", exc)
        record_source_result("gsccca", False, 0, str(exc))
        return []




# SOURCE 3: LEGAL NOTICES (foreclosure / probate / tax sale advertisements)
# =============================================================================
# Georgia uses non-judicial foreclosure. The operative public signal is the
# "Notice of Sale Under Power" advertised in the county's legal organ (The
# Rockdale Citizen for Rockdale, per O.C.G.A. 9-13-140), NOT a recorded "NOFC"
# document.
#
# Primary: georgiapublicnotice.com -- Georgia Press Association aggregator, free,
# filterable by county + category + date range. Rockdale Citizen notices land here.
# Fallback: none -- the Citizen publishes no stable PDF index of its legals.

NOTICE_CATEGORIES = ["Foreclosures", "Tax Sales", "Probate Notices",
                     "Sheriff/Marshal Sales", "Public Sales/Auctions",
                     "Debtors/Creditors"]

CATEGORY_TO_CAT = {
    "foreclosures": "FC",
    "tax sales": "TAX",
    "probate notices": "PRO",
    "sheriff/marshal sales": "FC",
    "sheriff's/marshal's sales": "FC",
    "debtors/creditors": "PRO",
    "debtors and creditors": "PRO",
    # "public sales/auctions" is deliberately unmapped: those ads are mostly
    # estate, storage-unit and consignment auctions, so parse_notice_body's
    # content heuristic classifies each one by what it actually says.
}

# CRM tag taxonomy (Rell, 2026-10-03): every lead carries two tags --
# "<county>scraper" naming the source, and a lead-type tag from cat.
CAT_TO_TYPE_TAG = {
    "FC": "preforeclosure",
    "TAX": "taxlien",
    "PRO": "probate",
}
COUNTY_SCRAPER_TAG = COUNTY.lower().replace(" ", "") + "scraper"


def crm_type_tag(rec):
    """Lead-type CRM tag for a record, e.g. 'preforeclosure' or 'taxlien'."""
    cat = (rec.get("cat") or "").upper()
    return CAT_TO_TYPE_TAG.get(cat, cat.lower() or "unknown")

FORECLOSURE_MARKERS = [
    "SALE UNDER POWER", "NOTICE OF SALE UNDER POWER", "FORECLOSURE",
    "SECURITY DEED", "ATTORNEY IN FACT", "POWER OF SALE", "DEED UNDER POWER",
    "PUBLIC OUTCRY", "COURTHOUSE DOOR", "HIGHEST BIDDER", "DEBT SECURED",
    "DEFAULT", "INDEBTEDNESS", "REMAINING IN DEFAULT",
]

# Georgia foreclosure sales happen on the first Tuesday of the month, and the
# ads say exactly that instead of printing a date: "sold on the first Tuesday in
# October 2026". Resolve it to a real date -- the sale date is what drives
# urgency on the call list.
FIRST_TUESDAY_RE = re.compile(r"first\s+Tuesday\s+in\s+([A-Za-z]+)\,?\s*(20\d{2})?", re.I)
SALE_DATE_RE = re.compile(
    r"(?:sale\s+date\s*[:\-]?\s*|will\s+be\s+sold\s+.{0,80}?\bon\s+)"
    r"([A-Z][a-z]+\s+\d{1,2},?\s+20\d{2}|\d{1,2}/\d{1,2}/20\d{2})",
    re.I,
)

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


def first_tuesday(year: int, month: int) -> datetime:
    """Georgia courthouse-steps sale day."""
    d = datetime(year, month, 1)
    return d + timedelta(days=(1 - d.weekday()) % 7)  # Monday=0, Tuesday=1


def resolve_sale_date(body: str) -> Optional[datetime]:
    """Prefer an explicitly printed date; otherwise compute the advertised
    first Tuesday."""
    m = SALE_DATE_RE.search(body)
    if m:
        dt = parse_date(m.group(1))
        if dt:
            return dt
    ft = FIRST_TUESDAY_RE.search(body)
    if ft:
        month = MONTHS.get((ft.group(1) or "").lower())
        if month:
            year_txt = ft.group(2)
            if year_txt:
                year = int(year_txt)
            else:
                now = utcnow()  # no year printed: assume the next such month
                year = now.year if month >= now.month else now.year + 1
            try:
                return first_tuesday(year, month)
            except ValueError:
                return None
    return None


BOOK_PAGE_RE = re.compile(r"Deed\s+Book\s+([\w\-]+)\s*,?\s*Page\s+([\w\-]+)", re.I)
PRINCIPAL_RE = re.compile(
    r"(?:original\s+principal\s+amount\s+of|principal\s+balance\s+of|"
    r"indebtedness\s+in\s+the\s+(?:original\s+)?amount\s+of)\s*\$?\s*"
    r"([\d,]+(?:\.\d{2})?)", re.I)
BORROWER_RE = re.compile(
    r"(?:executed\s+by|given\s+by|granted\s+by|from)\s+([A-Z][A-Za-z'\.\- ]{3,60}?)"
    r"\s+to\s+", re.I)

# Each kind of legal notice names the owner in its own way. Foreclosure ads say
# "executed by"; a tax sale says the property was "levied on as the property
# of"; a probate notice names the estate. Only knowing the foreclosure phrasing
# meant every tax sale and probate notice was found and then thrown away.
OWNER_PATTERNS = [
    # --- foreclosure ----------------------------------------------------
    r"(?:security\s+deed\s+)?(?:executed|given|granted|made)\s+by\s+"
    r"([A-Z][A-Za-z'\.\-]*(?:[\s,]+(?:AND|&)?\s*[A-Z][A-Za-z'\.\-]*){0,4})",
    # Some legal organs phrase it "Security Deed from <borrower> to <lender>"
    # (Fayette County News). Without this, those notices parse no owner and
    # are dropped as unusable.
    r"security\s+deed\s+from\s+"
    r"([A-Z][A-Za-z'\.\-]*(?:\s+[A-Z][A-Za-z'\.\-]*){0,3})\s+to\s+",
    r"\bgrantor(?:s)?\s*(?:is|are|:)?\s*"
    r"([A-Z][A-Za-z'\.\-]*(?:\s+[A-Z][A-Za-z'\.\-]*){0,3})",
    r"\bborrower(?:s)?\s*(?:is|are|:)?\s*"
    r"([A-Z][A-Za-z'\.\-]*(?:\s+[A-Z][A-Za-z'\.\-]*){0,3})",
    # --- tax sale / levy -------------------------------------------------
    r"levied\s+on\s+as\s+the\s+property\s+of\s+"
    r"([A-Z][A-Za-z'\.\-]*(?:[\s,]+(?:AND|&)?\s*[A-Z][A-Za-z'\.\-]*){0,4})",
    r"(?:as\s+)?the\s+property\s+of\s+"
    r"([A-Z][A-Za-z'\.\-]*(?:[\s,]+(?:AND|&)?\s*[A-Z][A-Za-z'\.\-]*){0,4})",
    r"\bdefendant(?:s)?\s*(?:in\s+fi\.?\s*fa\.?)?\s*(?:is|are|:)?\s*"
    r"([A-Z][A-Za-z'\.\-]*(?:\s+[A-Z][A-Za-z'\.\-]*){0,3})",
    r"in\s+the\s+name\s+of\s+"
    r"([A-Z][A-Za-z'\.\-]*(?:\s+[A-Z][A-Za-z'\.\-]*){0,3})",
    r"assessed\s+(?:to|against)\s+"
    r"([A-Z][A-Za-z'\.\-]*(?:\s+[A-Z][A-Za-z'\.\-]*){0,3})",
    # --- barment of redemption -------------------------------------------
    # A tax-deed purchaser barring the right to redeem addresses the owner
    # directly: "TO: RUSSELL J. PARKER SR OR ANY UNKNOWN ESTATE
    # REPRESENTATIVE OR UNKNOWN HEIRS AT LAW; CURVIN L. PARKER OR ANY..."
    r"\bTO:\s*([A-Z][A-Za-z'.\-]*(?:\s+[A-Z][A-Za-z'.\-]*){0,4})",
    r"notice\s+is\s+hereby\s+given\s+to\s+([A-Z][A-Za-z'.\-]*(?:\s+[A-Z][A-Za-z'.\-]*){0,4})",
    # --- probate / estate ------------------------------------------------
    r"[Ee]state\s+of\s+"
    r"([A-Z][A-Za-z'\.\-]*(?:\s+[A-Z][A-Za-z'\.\-]*){0,3})",
    r"([A-Z][A-Za-z'\.\-]*(?:\s+[A-Z][A-Za-z'\.\-]*){0,3})\s*,?\s+deceased",
    r"[Ll]ate\s+of\s+\w+\s+County.{0,40}?"
    r"([A-Z][A-Za-z'\.\-]*(?:\s+[A-Z][A-Za-z'\.\-]*){0,3})",
    r"petition\s+of\s+"
    r"([A-Z][A-Za-z'\.\-]*(?:\s+[A-Z][A-Za-z'\.\-]*){0,3})",
]
# Note: name tokens allow a single character so middle initials survive.
OWNER_RES = [re.compile(pat, re.I) for pat in OWNER_PATTERNS]

# Words that mean the capture ran past the name into the lender, the law firm,
# or boilerplate. The candidate is trimmed at these rather than discarded --
# "MARCUS T DUNCAN to Mortgage Electronic" is a good name plus junk, not junk.
_OWNER_STOP = re.compile(
    r"\s+(?:to|as|of|in|and\s+recorded|hereinafter|dated|recorded|will|shall|"
    r"pursuant|whose|by|for|a\s+single|an\s+unmarried|"
    r"a\s+married\s+(?:person|man|woman)|conveying|securing|"
    # Barment notices repeat this boilerplate after every name.
    r"or\s+any\s+unknown|or\s+unknown|heirs\s+at\s+law|estate\s+representative|\bor\b)\s",
    re.I)
_NOT_AN_OWNER = re.compile(
    r"\b(BANK|MORTGAGE|SERVICING|TRUSTEE|ELECTRONIC|REGISTRATION|SYSTEMS|"
    r"ASSOCIATION|CLERK|SUPERIOR|COURT|ATTORNEY|LAW|LLP|FEDERAL|NATIONAL|"
    r"NOTICE|SALE|POWER|DEED|SECURITY|PURSUANT|VIRTUE|UNDERSIGNED|CREDITOR|"
    r"HEREINAFTER|WHEREAS|DEFAULT|INDEBTEDNESS|DESCRIBED|COMMISSIONER)\b", re.I)

# Restored: this was deleted by accident when the owner patterns were rewritten,
# which crashed every notice before it could be parsed.
NOTICE_NUM_RE = re.compile(
    r"\b(?:notice|ad|legal)\s*(?:no\.?|number|#)\s*[:\-]?\s*([\w\-]{4,20})", re.I)

_TITLE_NOISE = re.compile(
    r"^(?:the|a|an|said|certain|his|her|its|their)\s+", re.I)


def _clean_owner_candidate(raw: str) -> str:
    """Trim a capture back to just the name."""
    cand = clean_text(raw)
    # A full stop ends the name. "TERENCE B BELL. Levy date" is a name plus the
    # next sentence. A trailing initial keeps its period ("ANITA M."), and a
    # middle initial's period is not a sentence end ("Tommy H. Harris" must
    # survive intact), so the second cut refuses to fire after a lone capital.
    cut = re.search(r"(?<=[a-z])\.\s|(?<!\b[A-Z])\.\s+[A-Z][a-z]", cand)
    if cut:
        cand = cand[:cut.start() + 1]
    cand = cand.rstrip(". ")
    cut = _OWNER_STOP.search(" " + cand + " ")
    if cut:
        cand = (" " + cand + " ")[:cut.start()].strip()
    cand = _TITLE_NOISE.sub("", cand).strip(" ,.;:&-")
    # Drop a trailing fragment that is clearly not part of a name.
    parts = cand.split()
    while parts and _NOT_AN_OWNER.fullmatch(parts[-1] or ""):
        parts.pop()
    # A trailing lone lowercase letter is the stump of "c/o" ("Felton Moulder
    # c" from "... Moulder c/o Administrator ..."). Uppercase lone letters are
    # real middle initials ("ANITA M") and are kept.
    while parts and re.fullmatch(r"[a-z]", parts[-1] or ""):
        parts.pop()
    return " ".join(parts).strip(" ,.;:&-")


def extract_owner_from_notice(body: str) -> str:
    """
    Pull the owner's name out of a legal advertisement, whatever kind it is.
    Returns the first candidate that reads like a person or a real entity
    rather than a lender, a law firm, or a stray piece of boilerplate.
    """
    for rx in OWNER_RES:
        for m in rx.finditer(body):
            cand = _clean_owner_candidate(m.group(1))
            if not (4 <= len(cand) <= 70):
                continue
            if _NOT_AN_OWNER.search(cand) and not is_entity(cand):
                continue
            toks = [t for t in re.split(r"[\s,]+", cand) if t]
            if len(toks) < 2 and not is_entity(cand):
                continue          # a single word is rarely a full name
            return cand
    return ""


def parse_notice_body(text: str, category_hint: str = "") -> Dict[str, Any]:
    """Pull structured fields out of a legal-advertisement body."""
    body = clean_text(text)
    # Notice bodies can be very long, and several owner patterns allow optional
    # repeats -- more than a few thousand characters risks the regex engine
    # backtracking for a long time. The name sits near the top regardless.
    probe = body[:4000]
    upper = body.upper()

    cat = CATEGORY_TO_CAT.get(category_hint.strip().lower(), "")
    if not cat:
        if any(m in upper for m in FORECLOSURE_MARKERS):
            cat = "FC"
        elif "TAX SALE" in upper or "TAX EXECUTION" in upper or "FI FA" in upper:
            cat = "TAX"
        elif any(m in upper for m in ("ESTATE OF", "EXECUTOR", "ADMINISTRATOR",
                                      "PROBATE", "YEAR'S SUPPORT", "YEARS SUPPORT")):
            cat = "PRO"
        else:
            cat = "UNK"

    sale_dt = resolve_sale_date(body)

    try:
        borrower = extract_owner_from_notice(probe)
    except Exception as exc:  # noqa: BLE001
        log.warning("    owner extraction failed: %s", exc)
        borrower = ""

    amount = None
    pm = PRINCIPAL_RE.search(body)
    if pm:
        amount = parse_money(pm.group(1))
    if amount is None:
        amount = parse_money(body) if "$" in body else None

    book = page_no = ""
    bpm = BOOK_PAGE_RE.search(body)
    if bpm:
        book, page_no = bpm.group(1), bpm.group(2)

    notice_no = ""
    nm = NOTICE_NUM_RE.search(body)
    if nm:
        notice_no = nm.group(1)

    lender = ""
    lm = re.search(r"(?:current\s+(?:secured\s+)?creditor|holder\s+of\s+the\s+security\s+deed)"
                   r"[^A-Za-z0-9]{0,20}(?:is\s+)?([A-Z][A-Za-z0-9'\.\,\- &]{4,70})", body, re.I)
    if lm:
        lender = clean_text(lm.group(1))

    # "Unknown heirs" means nobody has settled the estate -- a probate signal
    # sitting on top of a tax one, and worth surfacing on the lead.
    heirs_unknown = bool(re.search(
        r"unknown\s+heirs|estate\s+representative|unknown\s+estate", body, re.I))

    return {
        "cat": cat,
        "heirs_unknown": heirs_unknown,
        "owner": borrower,
        "prop_address": extract_address_from_text(body),
        "prop_zip": extract_zip_from_text(body),
        "amount": amount,
        "sale_date": sale_dt,
        "deed_book": book,
        "deed_page": page_no,
        "notice_number": notice_no,
        "lender": lender,
        "legal": body[:600],
    }


# Weekday publication dates, e.g. "Wednesday, October 1, 2025". These move
# with every weekly republication, so they are stripped before hashing.
_PUB_DATE_RE = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"[A-Z][a-z]+ \d{1,2}, 20\d{2}")


def _strip_notice_header(text: str) -> str:
    """
    Remove the parts of a legal ad that change with every republication --
    the leading ad code / site marker and the weekday publication dates --
    so weekly re-runs of one notice hash identically.
    """
    body = re.sub(r"^[A-Z]{2,4}\d{4,6}\s+(?:GPN\d+\s+)?", "", text, flags=re.I)
    return clean_text(_PUB_DATE_RE.sub("", body))


class LegalNoticeScraper:
    """
    georgiapublicnotice.com -- the Georgia Press Association notice database.

    The interaction was rebuilt from screenshots of the real site, which
    corrected three wrong guesses:

      1. You do not type a phrase. You pick FORECLOSURES from the "POPULAR
         SEARCHES" dropdown, which puts the single word "Foreclosures" in the
         search box.
      2. The county filter is NOT a checkbox. It is a list of rows that take a
         checkmark when clicked, and the whole COUNTY panel starts collapsed
         behind a "+" toggle that has to be opened first.
      3. The date range is a set of radio buttons -- "In the last N days" --
         rather than a pair of date fields.

    CAPTCHA: individual notice detail pages can present one. This scraper never
    attempts to solve, evade, or work around it. It reads only the search
    results list, which is not gated, and takes the borrower's name from the
    snippet. The property and mailing address then come from the county parcel
    roll we already hold -- which is more reliable than the notice text anyway.
    """

    CATEGORIES = [("Foreclosures", "FC"), ("Tax Sales", "TAX"),
                  ("Probate Notices", "PRO"),
                  ("Sheriff/Marshal Sales", "FC"),
                  ("Public Sales/Auctions", ""),
                  ("Debtors/Creditors", "PRO")]

    def __init__(self, start: datetime, end: datetime) -> None:
        self.start = start
        self.end = end
        self.county_filtered = False
        self.notes: List[str] = []

    # ------------------------------------------------------------- UI helpers
    @staticmethod
    async def _expand_panel(page, label: str) -> bool:
        """Open a collapsed filter panel (COUNTY, DATE RANGE ...)."""
        try:
            opened = await page.evaluate(
                """(label) => {
                    const rows = Array.from(document.querySelectorAll('div,td,th,a,span'));
                    for (const r of rows) {
                        const t = (r.innerText || '').trim().toUpperCase();
                        if (!t.startsWith(label)) continue;
                        // The +/- toggle is the nearest clickable in this row.
                        const host = r.closest('div,tr') || r;
                        const tog = host.querySelector(
                            'a,img,input[type=button],input[type=image],span.ui-icon,button');
                        if (tog) { tog.click(); return true; }
                        r.click();
                        return true;
                    }
                    return false;
                }""", label.upper())
            if opened:
                await page.wait_for_timeout(900)
            return bool(opened)
        except Exception as exc:  # noqa: BLE001
            log.debug("  expand %s failed: %s", label, exc)
            return False

    async def _select_county(self, page, county: str) -> bool:
        """
        Tick the county. The rows are not checkboxes, so a real checkbox is used
        when one exists and the row itself is clicked otherwise.
        """
        await self._expand_panel(page, "COUNTY")
        try:
            hit = await page.evaluate(
                """(county) => {
                    const want = county.toUpperCase();
                    // A genuine checkbox, if the markup happens to have one.
                    for (const cb of document.querySelectorAll('input[type=checkbox]')) {
                        const lbl = (cb.parentElement?.innerText || cb.value || '').trim().toUpperCase();
                        if (lbl === want) { if (!cb.checked) cb.click(); return 'checkbox'; }
                    }
                    // Otherwise the row itself is the control.
                    const rows = Array.from(document.querySelectorAll('li,div,td,a,span'));
                    for (const r of rows) {
                        const t = (r.innerText || '').trim().toUpperCase();
                        if (t !== want) continue;
                        if (r.offsetParent === null) continue;
                        r.click();
                        return 'row';
                    }
                    return null;
                }""", county)
            if hit:
                await page.wait_for_timeout(1200)
                log.info("  %s county selected (via %s)", county, hit)
                return True
        except Exception as exc:  # noqa: BLE001
            log.debug("  county select failed: %s", exc)
        self.notes.append(f"could not select county {county}")
        return False

    async def _set_last_days(self, page, days: int) -> bool:
        """Choose the 'In the last N days' radio and set N."""
        await self._expand_panel(page, "DATE RANGE")
        try:
            ok = await page.evaluate(
                """(days) => {
                    const radios = Array.from(document.querySelectorAll('input[type=radio]'));
                    for (const r of radios) {
                        const row = r.closest('div,tr,td,label') || r.parentElement;
                        const t = (row?.innerText || '').toLowerCase();
                        if (t.includes('in the last') && t.includes('day')) {
                            r.click();
                            const box = row.querySelector('input[type=text]');
                            if (box) {
                                box.value = String(days);
                                box.dispatchEvent(new Event('input',  {bubbles:true}));
                                box.dispatchEvent(new Event('change', {bubbles:true}));
                            }
                            return true;
                        }
                    }
                    return false;
                }""", days)
            if ok:
                await page.wait_for_timeout(700)
                log.info("  date range set to the last %d days", days)
                return True
        except Exception as exc:  # noqa: BLE001
            log.debug("  date range failed: %s", exc)
        self.notes.append("could not set the date range")
        return False

    @staticmethod
    async def _choose_category(page, label: str) -> bool:
        """Pick a category from the POPULAR SEARCHES dropdown."""
        try:
            ok = await page.evaluate(
                """(label) => {
                    const want = label.toUpperCase();
                    for (const sel of document.querySelectorAll('select')) {
                        for (const opt of sel.options) {
                            if ((opt.text || '').trim().toUpperCase() === want) {
                                sel.value = opt.value;
                                sel.dispatchEvent(new Event('change', {bubbles: true}));
                                return true;
                            }
                        }
                    }
                    return false;
                }""", label)
            if ok:
                await page.wait_for_timeout(1500)
            return bool(ok)
        except Exception:  # noqa: BLE001
            return False

    @staticmethod
    async def _submit(page) -> bool:
        """
        Press the search button.

        Guessing at this has failed repeatedly, so the page is asked what it
        actually has: every clickable control is listed, logged, and then tried
        in order of how much it looks like a search button. Pressing Enter in
        the search box is tried first, because a form that submits on Enter
        needs no button at all.
        """
        # 1. Enter in the keyword box.
        for sel in ("input[id*='txtSearch' i]", "input[type='search']",
                    "input[id*='Keyword' i]", "input[name*='search' i]"):
            try:
                box = page.locator(sel).first
                if await box.count() and await box.is_visible():
                    await box.press("Enter")
                    await page.wait_for_timeout(2500)
                    log.info("    submitted by pressing Enter in the search box")
                    return True
            except Exception:  # noqa: BLE001
                continue

        # 2. Inventory what is clickable and rank it.
        try:
            controls = await page.evaluate(
                """() => Array.from(document.querySelectorAll(
                        'a,button,input[type=image],input[type=submit],input[type=button]'))
                    .filter(e => e.offsetParent !== null)
                    .slice(0, 120)
                    .map((e, i) => {
                        if (!e.id) e.setAttribute('data-sub-id', 'sub' + i);
                        return {
                            sel: e.id ? '#' + CSS.escape(e.id)
                                      : '[data-sub-id="sub' + i + '"]',
                            tag: e.tagName,
                            type: e.getAttribute('type') || '',
                            id: e.id || '',
                            cls: e.className || '',
                            src: e.getAttribute('src') || '',
                            alt: e.getAttribute('alt') || '',
                            title: e.getAttribute('title') || '',
                            text: (e.innerText || e.value || '').trim().slice(0, 40)
                        };
                    })"""
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("    could not inspect the page: %s", exc)
            return False

        def rank(c: Dict[str, Any]) -> int:
            blob = " ".join(str(c.get(k, "")) for k in
                            ("id", "cls", "src", "alt", "title", "text")).lower()
            score = 0
            for word, pts in (("search", 40), ("magnif", 30), ("glass", 30),
                              ("submit", 25), ("go", 5), ("find", 15)):
                if word in blob:
                    score += pts
            for word in ("reset", "clear", "cancel", "help", "sign", "login",
                         "home", "about", "advanced"):
                if word in blob:
                    score -= 30
            if c.get("type") in ("image", "submit"):
                score += 20
            return score

        ranked = sorted(controls, key=rank, reverse=True)
        log.info("    %d clickable controls; best candidates: %s",
                 len(controls),
                 " | ".join(f"{c['tag']}{'#'+c['id'] if c['id'] else ''}"
                            f"{'/'+c['type'] if c['type'] else ''}"
                            f"{' '+c['text'][:18] if c['text'] else ''}"
                            f"{' src='+c['src'][-18:] if c['src'] else ''}"
                            for c in ranked[:6]))

        for c in ranked[:8]:
            if rank(c) <= 0:
                break
            try:
                await page.locator(c["sel"]).first.click(timeout=4000)
                log.info("    clicked %s (%s)", c["sel"],
                         c["text"] or c["alt"] or c["src"][-24:] or c["cls"][:24])
                await page.wait_for_timeout(2500)
                return True
            except Exception:  # noqa: BLE001
                continue

        log.warning("    nothing on the page looked like a search button")
        return False

    @staticmethod
    async def _settled_content(page, tries: int = 5) -> str:
        """
        Return the page HTML, waiting out any navigation first.

        Pressing Enter submits this form by starting a postback, and asking for
        content mid-flight fails with "the page is navigating and changing the
        content". That is not a failed search -- it is reading too early.
        """
        last = ""
        for attempt in range(tries):
            for state in ("domcontentloaded", "networkidle"):
                try:
                    await page.wait_for_load_state(state, timeout=15000)
                except Exception:  # noqa: BLE001
                    pass
            try:
                html = await page.content()
                # A results page is substantially bigger than the empty form.
                if len(html) > len(last):
                    last = html
                if len(html) > 20000:
                    return html
            except Exception as exc:  # noqa: BLE001
                if "navigat" not in str(exc).lower():
                    log.debug("    content read failed: %s", exc)
            await page.wait_for_timeout(2500 * (attempt + 1))
        return last

    # --------------------------------------------------------------- parsing
    _last_page_text: str = ""

    # The site's own category menu lists "Foreclosures", "Tax Sales" and
    # "Probate Notices" one after another, so the navigation panel matched the
    # notice markers and was parsed as if it were an advertisement.
    NAV_WORDS = ("popular searches", "advanced search", "alcoholic beverage",
                 "forfeiture/seizure", "name changes", "election notices",
                 "annual reports", "construction/service bids", "condemnations",
                 "public hearings", "smart search", "legal organ list")

    # A real legal advertisement always carries at least one of these. A menu
    # never does.
    REAL_NOTICE = re.compile(
        # Every advertisement on this site carries a header like
        # "City: Decatur County: DeKalb 430-186868 8/6, 8/13, 8/20, 8/27".
        # That header is the most dependable marker there is -- far better
        # than hoping for particular legal phrasing.
        r"County:\s*[A-Z][a-z]+|City:\s*[A-Z][a-z]+|\b\d{3}-\d{5,7}\b|"
        r"\bpursuant\b|under and by virtue|\bwhereas\b|deed book|"
        r"security deed|public outcry|courthouse door|highest bidder|"
        r"\bhereby\b|\blevied\b|\bexecut(?:ed|or|rix)\b|"
        r"\$\s?[\d,]{3,}|\b20\d{2}\b.{0,40}\b(?:deed|book|page|parcel|fi\.? ?fa)\b",
        re.I)

    # "City: Decatur County: DeKalb 430-186868" -- DeKalb-style numeric ids.
    # Rockdale ads carry letter codes instead: "City: Jonesboro County:
    # Rockdale CND7862". The trailing id is optional either way.
    HEADER_RE = re.compile(
        r"City:\s*([A-Za-z .'-]{3,30}?)\s+County:\s*([A-Za-z]{3,20})"
        r"(?:\s+(\d{3}-\d{5,7}|[A-Za-z]{2,4}\d{4,6}))?", re.I)

    # The letter-code an ad leads with, e.g. "CND7862 GPN11 NOTICE OF ...".
    # "GPN\d+" is the site's own category marker, not the ad's id.
    AD_CODE_RE = re.compile(r"\b(?!GPN)([A-Z]{2,4}\d{4,6})\b", re.I)

    @classmethod
    def _is_navigation(cls, text: str) -> bool:
        low = text.lower()
        hits = sum(1 for w in cls.NAV_WORDS if w in low)
        return hits >= 3

    # Cast wide: a legal advertisement is recognisable by any of these, and a
    # missed notice costs far more than an extra block to filter out later.
    NOTICE_MARKERS = re.compile(
        r"SALE UNDER POWER|POWER OF SALE|DEED UNDER POWER|FORECLOS|"
        r"SECURITY DEED|ATTORNEY IN FACT|PUBLIC OUTCRY|COURTHOUSE DOOR|"
        r"HIGHEST BIDDER|DEBT SECURED|INDEBTEDNESS|"
        r"TAX SALE|TAX EXECUTION|FI\.? ?FA|FIERI FACIAS|LEVY AND SALE|"
        r"EXCESS FUNDS|RIGHT TO REDEEM|"
        r"LIS PENDENS|"
        r"ESTATE OF|EXECUT(?:OR|RIX)|ADMINISTRAT(?:OR|RIX)|YEAR'?S SUPPORT|"
        r"LETTERS TESTAMENTARY|NOTICE TO DEBTORS|PETITION", re.I)

    @staticmethod
    def _parse_results(html: str, cat: str) -> List[Dict[str, Any]]:
        """
        Read the results list.

        The VIEW control is an ASP.NET postback rather than a link, so there is
        no Details.aspx href to key on -- that assumption is what returned zero
        notices. Instead each result block is found by its own content: a chunk
        of text long enough to be a notice and carrying a recognizable legal
        phrase. The snippet reliably contains the grantor (borrower) name, which
        is the only field the parcel match actually needs.
        """
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "nav", "header"]):
            tag.decompose()

        out: List[Dict[str, Any]] = []
        seen: set = set()
        stage = defaultdict(int)

        # Prefer tight containers so two notices never merge into one block.
        candidates = soup.select("tr, li, div")
        stage["candidate blocks"] = len(candidates)
        passing: List[Tuple[Any, str]] = []
        for block in candidates:
            text = clean_text(block.get_text(" "))
            if not (150 <= len(text) <= 6000):
                stage["wrong length"] += 1
                continue
            if not LegalNoticeScraper.NOTICE_MARKERS.search(text):
                stage["no legal phrase"] += 1
                continue
            if LegalNoticeScraper._is_navigation(text):
                stage["site menu, not a notice"] += 1
                continue
            if not LegalNoticeScraper.REAL_NOTICE.search(text):
                stage["no advertisement wording"] += 1
                continue
            stage["matched a legal phrase"] += 1
            # Skip a container that merely wraps other candidate blocks.
            inner = block.find_all(["tr", "li"])
            if len(inner) > 2:
                stage["wrapper, not a single notice"] += 1
                continue
            passing.append((block, text))

        # An ad nested inside another passing block is the same ad twice --
        # keep only the innermost block (7 ads were becoming 14 records).
        passing_ids = {id(b) for b, _ in passing}
        blocks: List[Tuple[Any, str]] = []
        for block, text in passing:
            if any(id(p) in passing_ids for p in block.parents):
                stage["outer wrapper of another notice"] += 1
                continue
            blocks.append((block, text))

        for block, text in blocks:
            sig = sha_key(text[:260])
            if sig in seen:
                continue
            seen.add(sig)

            notice_id, notice_city = "", ""
            hdr = LegalNoticeScraper.HEADER_RE.search(text)
            if hdr:
                notice_city = clean_text(hdr.group(1)).title()
                notice_id = clean_text(hdr.group(3) or "")
            if not notice_id:
                # Rockdale ads lead with their code (e.g. "ABC1234 GPN11 ...") --
                # take it as the stable ad id. The generic letter-code pattern
                # below covers whatever prefix the Rockdale Citizen uses.
                m = LegalNoticeScraper.AD_CODE_RE.search(text[:600])
                if m:
                    notice_id = m.group(1).upper()
            if not notice_id:
                m = re.search(r"[?&]ID=(\d+)", str(block))
                if m:
                    notice_id = m.group(1)
            if not notice_id:
                m = re.search(r"\b(\d{3}-\d{5,7}|GA\d{10,}|\d{2}-\d{3,5})\b", text)
                notice_id = m.group(1) if m else ""
            if not notice_id:
                # Last resort: hash the body with the dated masthead stripped,
                # so the weekly republications of one notice hash identically
                # and are not re-exported as new under NEW_ONLY.
                notice_id = sha_key(_strip_notice_header(text))

            out.append({"notice_id": notice_id, "city": notice_city,
                        "url": LEGAL_NOTICE_SEARCH_URL,
                        "text": text, "cat": cat})

        if not out:
            # Say plainly whether the notices are on the page at all. "The page
            # text starts with the nav menu" was never going to answer that.
            whole = clean_text(soup.get_text(" "))
            LegalNoticeScraper._last_page_text = whole
            log.info("    page holds %d characters of text", len(whole))
            for probe in ("SALE UNDER POWER", "FORECLOSURE", "SECURITY DEED",
                          "Search Results", "No records", "no notices found"):
                if re.search(probe, whole, re.I):
                    log.info("    page contains %r", probe)
            hit = re.search(r"SALE UNDER POWER|FORECLOSURE", whole, re.I)
            if hit:
                lo = max(0, hit.start() - 150)
                log.info("    text around the first hit: ...%s...",
                         whole[lo:hit.start() + 350])
            else:
                log.info("    no foreclosure wording anywhere -- the search "
                         "results did not load")
            log.info("    block filter: %s",
                     "; ".join(f"{k}={v}" for k, v in stage.items()))
        return out

    GRANTOR_RE = re.compile(
        r"(?:executed|given|granted)\s+by\s+([A-Z][A-Za-z'\.\-]+(?:\s+[A-Z][A-Za-z'\.\-]+){0,3})",
        re.I)

    def _to_lead_safe(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return self._to_lead(item)
        except Exception:  # noqa: BLE001
            return None

    def _to_lead(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text, cat = item["text"], item["cat"]
        parsed = parse_notice_body(text, "")
        owner = parsed["owner"]
        if not owner:
            m = self.GRANTOR_RE.search(text)
            if m:
                owner = clean_text(m.group(1))
        # These snippets are truncated, and the borrower's name is often cut
        # off. When the property address survives instead, that is just as
        # good: the parcel roll turns an address into an owner and a mailing
        # address, which is the direction that actually matters.
        if not owner and not parsed.get("prop_address"):
            return None

        pub_date = None
        dm = re.search(r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
                       r"([A-Z][a-z]+ \d{1,2}, 20\d{2})", text)
        if dm:
            pub_date = parse_date(dm.group(1))

        return {
            "doc_num": f"GPN-{item['notice_id']}",
            "doc_type": "Notice of Sale Under Power" if cat == "FC" else f"Legal Notice ({cat})",
            "filed": fmt_date(pub_date or utcnow().replace(tzinfo=None)),
            "cat": cat,
            "cat_label": CAT_LABELS.get(cat, cat),
            "owner": owner,
            "grantee": parsed["lender"],
            "amount": parsed["amount"],
            "legal": text[:600],
            "parcel_id": "",
            "prop_address": parsed["prop_address"],
            "prop_city": "",
            # The "City:" in an ad header is the newspaper's city (the legal
            # organ), never the property's -- keep it separate.
            "pub_city": item.get("city", ""),
            "name_order": "natural",
            "prop_zip": parsed["prop_zip"],
            "clerk_url": item["url"],
            "source": "Georgia Public Notice (legal organ advertisement)",
            "foreclosure_sale_date": fmt_date(parsed["sale_date"]) or None,
            "notice_number": item["notice_id"],
            "heirs_unknown": parsed.get("heirs_unknown", False),
            "status": "active",
            "is_release": False,
            "_notice_dedupe": item["notice_id"],
        }

    # ------------------------------------------------------------------ main
    async def run(self) -> List[Dict[str, Any]]:
        from playwright.async_api import async_playwright

        results: List[Dict[str, Any]] = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
            ctx = await browser.new_context(
                viewport={"width": 1500, "height": 1000},
                user_agent=USER_AGENT, locale="en-US", timezone_id="America/New_York")
            ctx.set_default_timeout(15000)
            page = await ctx.new_page()
            page.set_default_navigation_timeout(NAV_TIMEOUT_MS)

            try:
                for label, cat in self.CATEGORIES:
                    try:
                        rows = await self._run_category(page, label, cat)
                        results.extend(rows)
                    except Exception as exc:  # noqa: BLE001
                        log.warning("  %s search failed: %s", label, str(exc)[:160])
                        self.notes.append(f"{label}: {str(exc)[:160]}")
                        try:    # leave a clean page for the next category
                            await page.goto(LEGAL_NOTICE_SEARCH_URL,
                                            wait_until="domcontentloaded")
                        except Exception:  # noqa: BLE001
                            pass
                    await page.wait_for_timeout(int(POLITE_DELAY * 1000))
            finally:
                await ctx.close()
                await browser.close()
        return results

    async def _run_category(self, page, label: str, cat: str) -> List[Dict[str, Any]]:
        log.info("Legal notices: %s in %s County, last %d days",
                 label, COUNTY, LOOKBACK_DAYS)
        await page.goto(LEGAL_NOTICE_SEARCH_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(2500)

        if not await self._choose_category(page, label):
            self.notes.append(f"could not pick {label} from the dropdown")
            log.warning("  could not pick %s from the category dropdown", label)
            return []

        self.county_filtered = await self._select_county(page, COUNTY)
        await self._set_last_days(page, max(LOOKBACK_DAYS, 30))

        if not await self._submit(page):
            self.notes.append("could not press the search button")
            log.warning("  could not press the search button")
            return []

        # The postback can fire more than one navigation; let it finish.
        await page.wait_for_timeout(3000)
        try:
            await page.wait_for_load_state("networkidle", timeout=25000)
        except Exception:  # noqa: BLE001
            await page.wait_for_timeout(6000)

        out: List[Dict[str, Any]] = []
        for page_no in range(1, 11):        # up to 10 result pages per category
            html = await self._settled_content(page)
            log.info("    results page is %d characters", len(html))
            items = self._parse_results(html, cat)
            log.info("  page %d: %d notices", page_no, len(items))
            if not items:
                # Capture what the page actually contained. Guessing at this
                # blind has cost enough rounds already.
                if page_no == 1:
                    try:
                        body = clean_text(
                            BeautifulSoup(html, "lxml").get_text(" "))[:4000]
                        safe_write_json(DATA_DIR / "notice_page_sample.json", {
                            "captured_at": utcnow().isoformat(),
                            "category": label, "url": page.url,
                            "html_length": len(html),
                            "visible_text": body,
                        })
                        log.info("  wrote data/notice_page_sample.json")
                    except Exception as exc:  # noqa: BLE001
                        log.debug("  notice sample failed: %s", exc)
                break
            no_owner, errored, first_err = 0, 0, ""
            for item in items:
                try:
                    lead = self._to_lead(item)
                    if lead:
                        out.append(lead)
                    else:
                        no_owner += 1
                except Exception as exc:  # noqa: BLE001
                    errored += 1
                    if not first_err:
                        first_err = f"{type(exc).__name__}: {exc}"
            if no_owner:
                log.info("    %d notice(s) had neither a name nor an address in "
                         "the snippet (the full text is behind the site's captcha)",
                         no_owner)
            by_addr = sum(1 for r in out if not r.get("owner") and r.get("prop_address"))
            if by_addr:
                log.info("    %d notice(s) matched by property address instead "
                         "of name", by_addr)
            if errored:
                # Never hide this again. A crash here looks exactly like "found
                # nothing", and that cost a full round of debugging.
                log.warning("    %d notice(s) raised an error -- first was %s",
                            errored, first_err[:300])
            if (no_owner or errored) and items:
                # Show a notice that genuinely failed, so the sample is the
                # wording that needs handling rather than whatever came first.
                failing = next(
                    (it for it in items
                     if not self._to_lead_safe(it)), items[0])
                log.info("    example of one we could not read: %s",
                         clean_text(failing.get("text", ""))[:400])

            advanced = False
            for sel in ("a[title*='Next' i]", "a:has-text('>')",
                        "input[type=image][alt*='Next' i]", "a[id*='Next' i]"):
                try:
                    nxt = page.locator(sel).first
                    if await nxt.count() and await nxt.is_visible():
                        await nxt.click()
                        await page.wait_for_timeout(int(POLITE_DELAY * 1000) + 1500)
                        advanced = True
                        break
                except Exception:  # noqa: BLE001
                    continue
            if not advanced:
                break

        log.info("  %s: %d usable notices", label, len(out))
        return out


def scrape_champion_fallback(session: requests.Session) -> List[Dict[str, Any]]:
    """No legal-organ PDF fallback is configured for Rockdale County.

    (DeKalb's scraper falls back to The Champion's weekly legal-section PDFs;
    Rockdale's legal organ, the Rockdale Citizen, publishes no stable PDF
    index, so the georgiapublicnotice.com pass above is the only notice source.)
    """
    log.info("No legal-organ PDF fallback configured for Rockdale County; skipping")
    record_source_result("legal_organ_pdfs", True, 0, "not configured")
    return []


async def scrape_legal_notices(start: datetime, end: datetime,
                               session: requests.Session) -> List[Dict[str, Any]]:
    if "NOTICES" in SKIP_SOURCES:
        log.info("Skipping legal notices (SKIP_SOURCES)")
        record_source_result("legal_notices", True, 0, "skipped")
        return []
    rows: List[Dict[str, Any]] = []
    if await ensure_browser():
        scraper = LegalNoticeScraper(start, end)
        try:
            rows = await aretry(scraper.run, times=2, label="legal-notices")
            record_source_result("legal_notices", True, len(rows))
        except Exception as exc:  # noqa: BLE001
            log.error("Legal notice source failed: %s", exc)
            record_source_result("legal_notices", False, 0, str(exc))
    else:
        record_source_result("legal_notices", False, 0, BROWSER_ERROR)

    if not rows:
        log.info("Trying The Champion PDF fallback")
        rows = scrape_champion_fallback(session)
    return rows


# =============================================================================
# =============================================================================
# SOURCE 4: ROCKDALE COUNTY TAX COMMISSIONER -- TAX SALE LISTING
# =============================================================================
# Rockdale posts the current tax-sale property list as a document LINKED FROM
# its tax-sales page (https://rockdaletaxoffice.org/property-tax-sales),
# published ~4 weeks before each sale and taken down afterwards -- there is no
# fixed listing URL. The scraper scans the page for listing links each run
# and parses the newest one. Between sales it finds no listing link and
# reports zero rows, not a failure.
#
# Rockdale's list layout (verified 2026-10-03 from the crawled April 1, 2025
# and May 4, 2021 sale lists):
#
#   FILE # | YEARS | PARCEL | OWNER (+", IN REM") | OPENING BID (taxes due)
#
# e.g. "| 337 2019 032A010003 URBAN PROPERTY SOLUTIONS LLC, IN REM | $ 2,357.68 |"
# The sale date comes from the document title ("APRIL 1, 2025"). The parser
# first tries the positional layout; if it yields zero rows, a defensive
# fallback extracts any Rockdale-style parcel tokens ("0690010241",
# "045B010022", "C380010164") from the raw text. Re-verify against the live
# October 6, 2026 sale list if the positional parse comes back empty.

TAX_PDF_MONTHS = {
    # Month names for ranking listing filenames by date. The listing links are
    # discovered on the tax-sales page each run (see _tax_pdf_links) and the
    # newest one wins (see _rank_tax_pdfs); test_regression.py covers both.
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
DATE_WORD_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
# A listing document that reads "no active sales" (or "we currently do not
# have any active tax sales") is the county's own empty state -- zero rows,
# not a failure. Also notes the next sale when the document names one.
_NO_ACTIVE_SALES_RE = re.compile(
    r"no\s+active\s+(tax\s+)?sales?"
    r"|do\s+not\s+have\s+any\s+active\s+(tax\s+)?sales?",
    re.I)
# "Our next sale will be in August 2027" -- logged so the run summary says
# when to expect the next list.
_NEXT_SALE_RE = re.compile(
    r"next\s+sale\s+will\s+be\s+in\s+([A-Za-z]+)\s+(20\d{2})", re.I)


def _tax_pdf_links(session: requests.Session) -> List[Tuple[str, str, str]]:
    """
    Scan the Tax Commissioner's tax-sales info page for links to the current
    tax-sale property list; return [(name, url, modified)].

    Rockdale posts the list as a document linked from
    https://rockdaletaxoffice.org/property-tax-sales (published ~4 weeks
    before each sale, taken down afterwards). Between sales the page carries
    no listing link and this returns [] -- the caller reports that as zero
    rows, not a failure.
    """
    from urllib.parse import urljoin

    resp = session.get(TAX_SALE_URL, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    links: List[Tuple[str, str, str]] = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = clean_text(a["href"])
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            continue
        text = clean_text(a.get_text(" "))
        name = href.rsplit("/", 1)[-1] or text
        is_pdf = href.lower().endswith(".pdf")
        looks = _looks_like_tax_listing(name) or _looks_like_tax_listing(text)
        if not (is_pdf or looks):
            continue
        url = urljoin(TAX_SALE_URL, href)
        if url in seen:
            continue
        seen.add(url)
        links.append((name, url, _listing_modified(a)))
    # A direct document URL seeded in TAX_SALE_LISTING_URLS is always a
    # candidate too, in case the county ever publishes one at a stable address.
    for seed in TAX_SALE_LISTING_URLS:
        if seed not in seen:
            links.append((seed.rsplit("/", 1)[-1], seed, ""))
    return links


_LISTING_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})\s+\d{1,2}:\d{2}\s*[AP]M", re.I)


def _listing_modified(anchor: Any) -> str:
    """
    Pull the "last modified" date shown next to a directory-listing link
    (IIS style: "9/9/2026  8:56 AM  72079 <name>"). Returns "YYYY-MM-DD"
    or "" when the listing carries no date.
    """
    bits = []
    for sib in anchor.previous_siblings:
        if getattr(sib, "name", None) in ("br", "a"):
            break
        bits.append(sib.get_text() if hasattr(sib, "get_text") else str(sib))
    m = _LISTING_DATE_RE.search(" ".join(reversed(bits)))
    if m:
        return f"{int(m.group(3)):04d}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return ""


# A tax-sale listing name mentions the tax sale; anything else in the
# directory (forms, backups of other documents) is not a candidate, no matter
# how new it is.
_TAX_LISTING_NAME_RE = re.compile(
    r"tax.{0,12}(sale|listing|delinq|levy|fi\.?\s?fa)|\btax\s+sale\b", re.I)


def _looks_like_tax_listing(name: str) -> bool:
    return bool(_TAX_LISTING_NAME_RE.search(name))


def _tax_pdf_rank(name: str) -> Tuple[int, int, str]:
    """Rank a listing filename by (year, month) parsed from the name."""
    low = name.lower()
    year = 0
    m = re.search(r"(19|20)\d{2}", low)
    if m:
        year = int(m.group(0))
    else:
        # Two-digit years: "9-8-26.pdf", "10-6-26.pdf" are 2026, not dateless.
        m2 = re.search(r"(?<![\d/])(\d{1,2})[-_](\d{1,2})[-_](\d{2})(?!\d)", low)
        if m2:
            year = 2000 + int(m2.group(3))
    month = 0
    for mname, mnum in TAX_PDF_MONTHS.items():
        if mname in low:
            month = mnum
            break
    if not month:
        m = re.search(r"(?<![\d/])(\d{1,2})[-_](\d{1,2})[-_]\d{2,4}", low)
        if m:
            month = int(m.group(1))
    return (year, month, low)


def _rank_tax_pdfs(links: List[Tuple[str, str, str]]) -> List[Tuple[str, str]]:
    """
    Newest-first [(filename, url)]. The directory listing's modified date
    beats the filename -- filenames lie ("NOVEMBER ... 9-8-26.pdf") and two
    different months' lists can share a posting day.
    """
    cands = [it for it in links if _looks_like_tax_listing(it[0])]
    cands.sort(key=lambda it: (it[2], *_tax_pdf_rank(it[0])), reverse=True)
    return [(n, u) for n, u, _ in cands]


def _cluster_words_by_row(words: List[Tuple]) -> List[List[Tuple]]:
    """Group pymupdf words into text lines by y-center."""
    lines: List[List[Tuple]] = []
    for w in sorted(words, key=lambda w: (round((w[1] + w[3]) / 2, 1), w[0])):
        ymid = (w[1] + w[3]) / 2
        placed = False
        for line in lines:
            lymid = (line[0][1] + line[0][3]) / 2
            if abs(lymid - ymid) <= 3.0:
                line.append(w)
                placed = True
                break
        if not placed:
            lines.append([w])
    for line in lines:
        line.sort(key=lambda w: w[0])
    return lines


_ROCKDALE_SALE_TITLE_RE = re.compile(
    r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|"
    r"NOVEMBER|DECEMBER)\s+(\d{1,2})(?:ST|ND|RD|TH)?,?\s+(20\d{2})", re.I)
# Rockdale prints owners as "NAME, IN REM" and often appends
# "ALL HEIRS KNOWN & UNKNOWN" -- neither is part of the owner's name.
_ROCKDALE_OWNER_STRIP_RE = re.compile(
    r",?\s*\bIN\s+REM\b\s*,?"
    r"|,?\s*\bALL\s+HEIRS\s+KNOWN\s*(?:&|AND)\s*UNKNOWN\b\s*,?", re.I)
# A bare Rockdale parcel token, e.g. "0690010241", "045B010022", "093A01079A",
# "C380010164". Used on isolated PDF word cells, so the shape check is the
# whole match.
_ROCKDALE_PARCEL_TOKEN_RE = re.compile(
    r"^(?:\d{9,10}|(?=[A-Z0-9]*[A-Z])[A-Z]?\d{3}[A-Z]?\d{3}\d{2,4}[A-Z]?)$")


def _rockdale_sale_date(full_text: str) -> str:
    """
    The sale date comes from the list's own title ("APRIL 1, 2025",
    "MAY 4, 2021 TAX SALE PROPERTIES"). Returns "YYYY-MM-DD" or "".
    """
    m = _ROCKDALE_SALE_TITLE_RE.search(full_text[:3000])
    if not m:
        return ""
    try:
        month = MONTHS[m.group(1).lower()]
        return f"{int(m.group(3)):04d}-{month:02d}-{int(m.group(2)):02d}"
    except (KeyError, ValueError):
        return ""


def _clean_rockdale_owner(raw: str) -> str:
    # Any dollar amount that leaked into the owner text is debris, not name.
    text = re.sub(r"\$\s*[\d,]*\.?\d+", " ", clean_text(raw))
    owner = _ROCKDALE_OWNER_STRIP_RE.sub("", text).strip(" ,")
    return normalize_name(owner)


def _parse_rockdale_layout(doc: Any, full_text: str,
                           source_url: str) -> Optional[List[Dict[str, Any]]]:
    """
    Parse Rockdale's tax-sale list layout:

        FILE # | YEARS | PARCEL | OWNER | OPENING BID (taxes due)

    Rows are anchored on parcel-id cells in the parcel column (there is no
    date column). Returns None when the document is NOT the Rockdale layout,
    so the caller can try the legacy positional parser instead.
    """
    first = doc[0].get_text("words")
    lines = _cluster_words_by_row(first)

    col_bounds: List[Tuple[float, float]] = []
    col_names: List[str] = []
    for line in lines:
        text = " ".join(w[4] for w in line).lower()
        if "parcel" not in text or ("file" not in text and "year" not in text):
            continue
        centers: Dict[str, float] = {}
        for w in line:
            wl = w[4].lower()
            cx = (w[0] + w[2]) / 2
            if "file" in wl or wl == "#":
                centers.setdefault("file", cx)
            elif "year" in wl:
                centers["years"] = cx
            elif "parcel" in wl:
                centers["parcel"] = cx
            elif wl in ("address", "location", "property", "owner", "name"):
                centers.setdefault("location", cx)
            elif wl in ("bid", "taxes", "due", "current", "opening", "amount"):
                centers.setdefault("bid", cx)
        if "parcel" in centers and len(centers) >= 3:
            ordered = sorted(centers.items(), key=lambda kv: kv[1])
            col_names = [k for k, _ in ordered]
            bounds = [c for _, c in ordered]
            col_bounds = [(-1.0, (bounds[0] + bounds[1]) / 2)]
            for i in range(1, len(bounds) - 1):
                col_bounds.append(((bounds[i - 1] + bounds[i]) / 2,
                                   (bounds[i] + bounds[i + 1]) / 2))
            col_bounds.append(((bounds[-2] + bounds[-1]) / 2, 1e9))
            break

    if not col_bounds:
        return None  # not the Rockdale layout; caller tries the legacy one

    sale_date = _rockdale_sale_date(full_text) or today_et()
    sale_dt = parse_date(sale_date)
    filed = fmt_date(sale_dt) or sale_date
    stamp = sale_dt.strftime("%Y%m") if sale_dt else "000000"
    pidx = col_names.index("parcel")
    p_lo, p_hi = col_bounds[pidx]

    out: List[Dict[str, Any]] = []
    for page in doc:
        words = page.get_text("words")
        plines = _cluster_words_by_row(words)
        # (row y-center, parcel token's right edge)
        anchors: List[Tuple[float, float]] = []
        for line in plines:
            for w in line:
                cx = (w[0] + w[2]) / 2
                if p_lo <= cx <= p_hi and \
                        _ROCKDALE_PARCEL_TOKEN_RE.match(w[4].strip().upper()):
                    anchors.append(((w[1] + w[3]) / 2, w[2]))
                    break
        anchors.sort()
        for i, (ymid, parcel_right) in enumerate(anchors):
            y_next = anchors[i + 1][0] if i + 1 < len(anchors) else 1e9
            row_words = sorted(
                (w for line in plines for w in line
                 if ymid - 4.0 <= (w[1] + w[3]) / 2 < y_next - 4.0),
                key=lambda w: w[0])
            cols: Dict[str, List[str]] = {name: [] for name in col_names}
            for w in row_words:
                cx = (w[0] + w[2]) / 2
                for (lo, hi), name in zip(col_bounds, col_names):
                    if lo <= cx <= hi:
                        cols[name].append(w[4])
                        break
            parcel_id = clean_text(" ".join(cols.get("parcel", [])))
            m = PARCEL_ID_IN_TEXT_RE.search(parcel_id.upper())
            parcel_id = clean_text(m.group(1)) if m else ""
            if not parcel_id:
                continue
            # Owner = everything right of the parcel token up to the money;
            # amount = the money itself. The amount words are removed from the
            # owner text by identity (not by position) so a long owner name
            # that runs under the bid column is never truncated mid-name.
            after = [w for w in row_words if w[0] > parcel_right - 1.0]
            mi = next((j for j, w in enumerate(after)
                       if w[4].strip().startswith("$")), None)
            if mi is not None:
                dollar = after[mi][4].strip()
                skip_ids = {id(after[mi])}
                if re.match(r"^\$[\d,]+\.\d{1,2}$", dollar):
                    amount = parse_money(dollar)
                else:
                    num_word = None
                    for w in after[mi + 1:mi + 4]:
                        if re.match(r"^[\d,]+\.\d{1,2}$", w[4].strip()):
                            num_word = w
                            break
                    amount = parse_money(
                        dollar + " " + (num_word[4] if num_word else ""))
                    if num_word is not None:
                        skip_ids.add(id(num_word))
                owner = _clean_rockdale_owner(
                    " ".join(w[4] for w in after if id(w) not in skip_ids))
            else:
                owner = _clean_rockdale_owner(" ".join(cols.get("location", [])))
                amount = parse_money(" ".join(cols.get("bid", [])))
            years_txt = re.sub(r"\s+", "", " ".join(cols.get("years", [])))
            out.append({
                "doc_num": f"TAX-{stamp}-{normalize_parcel_id(parcel_id)}",
                "doc_type": "Tax Sale / FiFa (ROCKDALE)",
                "filed": filed,
                "cat": "TAX",
                "cat_label": CAT_LABELS["TAX"],
                "owner": owner,
                "name_order": "last-first",
                "grantee": "Rockdale County Tax Commissioner",
                "amount": amount,
                "legal": (f"Rockdale County tax sale {filed}. "
                          f"Delinquent tax years {years_txt}. "
                          f"Opening bid "
                          f"{f'${amount:,.2f}' if amount else 'n/a'}.").strip(),
                "parcel_id": parcel_id,
                "prop_address": "",
                "clerk_url": source_url,
                "source": "Rockdale County Tax Commissioner (tax sale listing)",
                "foreclosure_sale_date": filed or None,
                "notice_number": parcel_id,
                "status": "active",
                "is_release": False,
                "tax_sale_date": filed,
                "years_delinquent": years_txt,
            })
    log.info("Rockdale tax sale PDF: %d parcel rows (Rockdale layout) from %s",
             len(out), source_url)
    return out


def _parse_tax_sale_pdf(pdf_bytes: bytes, source_url: str) -> List[Dict[str, Any]]:
    try:
        import fitz  # pymupdf; noqa: PLC0415
    except ImportError:
        log.error("pymupdf is not installed; cannot parse the Rockdale tax sale PDF")
        return []

    out: List[Dict[str, Any]] = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    # Between auctions the PDF is a single "no active sales" notice.
    full_text = " ".join(page.get_text("text") for page in doc)
    if _NO_ACTIVE_SALES_RE.search(full_text):
        nxt = _NEXT_SALE_RE.search(full_text)
        log.info("Rockdale tax sale PDF shows no active sales%s",
                 f" (next sale: {nxt.group(1)} {nxt.group(2)})" if nxt else "")
        return []

    # Rockdale's own list layout first; None means "not this layout".
    rockdale_rows = _parse_rockdale_layout(doc, full_text, source_url)
    if rockdale_rows is not None:
        out = rockdale_rows
    else:
        out = _parse_legacy_tax_layout(doc, source_url)

    if not out:
        # Positional parse found nothing. Defensive fallback: harvest
        # Rockdale-style parcel tokens from the raw text so a sale is never
        # silently dropped.
        out = _fallback_tax_parcels(full_text, source_url)
        if out:
            log.info("Rockdale tax sale PDF: positional parse empty; fallback "
                     "harvested %d parcel tokens", len(out))

    log.info("Rockdale tax sale PDF: %d parcel rows from %s", len(out), source_url)
    return out


def _parse_legacy_tax_layout(doc: Any, source_url: str) -> List[Dict[str, Any]]:
    """The Clayton-era positional parser (Date | Parcel#/Name | Property
    Location | Years | Fair Market Value | Cry-Out Bid), kept as a fallback
    in case the county ever publishes a list in that shape again."""
    out: List[Dict[str, Any]] = []
    col_bounds: List[Tuple[float, float]] = []
    col_names: List[str] = []

    for page in doc:
        words = page.get_text("words")  # x0, y0, x1, y1, word, ...
        lines = _cluster_words_by_row(words)

        if not col_bounds:
            for line in lines:
                text = " ".join(w[4] for w in line).lower()
                if "parcel" in text and ("location" in text or "property" in text):
                    # Map header words to our six columns by keyword.
                    centers: Dict[str, float] = {}
                    for w in line:
                        wl = w[4].lower()
                        cx = (w[0] + w[2]) / 2
                        if wl == "date":
                            centers["date"] = cx
                        elif "parcel" in wl:
                            centers["parcel"] = cx
                        elif wl in ("property", "location"):
                            centers.setdefault("location", cx)
                        elif wl == "years":
                            centers["years"] = cx
                        elif wl in ("fair", "market", "value"):
                            centers.setdefault("fmv", cx)
                        elif wl in ("cry-out", "cry", "bid"):
                            centers.setdefault("bid", cx)
                    if len(centers) >= 4:
                        ordered = sorted(centers.items(), key=lambda kv: kv[1])
                        col_names = [k for k, _ in ordered]
                        bounds = [c for _, c in ordered]
                        col_bounds = [(-1.0, (bounds[0] + bounds[1]) / 2)]
                        for i in range(1, len(bounds) - 1):
                            col_bounds.append(((bounds[i - 1] + bounds[i]) / 2,
                                               (bounds[i] + bounds[i + 1]) / 2))
                        col_bounds.append(((bounds[-2] + bounds[-1]) / 2, 1e9))
                    break
            if not col_bounds:
                continue  # no header on this page; try the next

        # Anchor logical rows on sale-date cells in the date column.
        date_col = col_names.index("date") if "date" in col_names else 0
        d_lo, d_hi = col_bounds[date_col]
        anchors: List[Tuple[float, List[Tuple]]] = []
        for line in lines:
            for w in line:
                cx = (w[0] + w[2]) / 2
                if d_lo <= cx <= d_hi and DATE_WORD_RE.match(w[4].strip()):
                    anchors.append((((w[1] + w[3]) / 2), line))
                    break
        anchors.sort(key=lambda a: a[0])

        for i, (ymid, _) in enumerate(anchors):
            y_next = anchors[i + 1][0] if i + 1 < len(anchors) else 1e9
            row_words = [w for line in lines for w in line
                         if ymid - 4.0 <= (w[1] + w[3]) / 2 < y_next - 4.0]
            cols: Dict[str, List[str]] = {name: [] for name in col_names}
            for w in row_words:
                cx = (w[0] + w[2]) / 2
                for (lo, hi), name in zip(col_bounds, col_names):
                    if lo <= cx <= hi:
                        cols[name].append(w[4])
                        break
            sale_date = " ".join(cols.get("date", [])).strip()
            parcel_name = " ".join(cols.get("parcel", [])).strip()
            location = " ".join(cols.get("location", [])).strip()
            years = " ".join(cols.get("years", [])).strip()
            fmv = parse_money(" ".join(cols.get("fmv", [])))
            bid = parse_money(" ".join(cols.get("bid", [])))
            if "/" in parcel_name:
                parcel_id, owner = parcel_name.split("/", 1)
            else:
                parcel_id, owner = parcel_name, ""
            parcel_id = clean_text(parcel_id)
            owner = normalize_name(owner)
            if not parcel_id:
                continue
            sale_dt = parse_date(sale_date)
            filed = fmt_date(sale_dt)
            years_txt = re.sub(r"\s+", "", years)
            # The same parcel repeats in every monthly list; without the sale
            # month in the key, dedupe collapses them into one document and
            # NEW_ONLY never re-exports the new month.
            stamp = sale_dt.strftime("%Y%m") if sale_dt else "000000"
            out.append({
                "doc_num": f"TAX-{stamp}-{normalize_parcel_id(parcel_id)}",
                "doc_type": "Tax Sale / FiFa (ROCKDALE)",
                "filed": filed,
                "cat": "TAX",
                "cat_label": CAT_LABELS["TAX"],
                "owner": owner,
                "name_order": "last-first",
                "grantee": "Rockdale County Tax Commissioner",
                "amount": bid,
                "legal": (f"Delinquent tax years {years_txt}. "
                          f"Fair market value "
                          f"{f'${fmv:,.0f}' if fmv else 'n/a'}.").strip(),
                "parcel_id": parcel_id,
                "prop_address": clean_text(location),
                "clerk_url": source_url,
                "source": "Rockdale County Tax Commissioner (tax sale listing)",
                "foreclosure_sale_date": filed or None,
                "notice_number": parcel_id,
                "status": "active",
                "is_release": False,
                "tax_sale_date": filed,
                "years_delinquent": years_txt,
            })

    return out


def _fallback_tax_parcels(full_text: str, source_url: str) -> List[Dict[str, Any]]:
    """Last-resort harvest of Rockdale parcel tokens ("W09 030", "111 1019 052")
    from raw PDF text when the positional parser yields zero rows."""
    out: List[Dict[str, Any]] = []
    seen = set()
    for m in PARCEL_ID_IN_TEXT_RE.finditer(full_text.upper()):
        parcel_id = clean_text(m.group(1))
        key = normalize_parcel_id(parcel_id)
        if not key or key in seen:
            continue
        seen.add(key)
        stamp = now_et().strftime("%Y%m")
        out.append({
            "doc_num": f"TAX-{stamp}-{key}",
            "doc_type": "Tax Sale / FiFa (ROCKDALE)",
            "filed": today_et(),
            "cat": "TAX",
            "cat_label": CAT_LABELS["TAX"],
            "owner": "",
            "name_order": "last-first",
            "grantee": "Rockdale County Tax Commissioner",
            "amount": 0.0,
            "legal": "Parcel appears on the Rockdale County tax sale list; "
                     "layout unverified -- confirm details on the PDF.",
            "parcel_id": parcel_id,
            "prop_address": "",
            "clerk_url": source_url,
            "source": "Rockdale County Tax Commissioner (tax sale listing)",
            "foreclosure_sale_date": None,
            "notice_number": parcel_id,
            "status": "active",
            "is_release": False,
            "tax_sale_date": today_et(),
            "years_delinquent": "",
        })
    return out


async def scrape_tax_sales(session: requests.Session) -> List[Dict[str, Any]]:
    if "TAX" in SKIP_SOURCES:
        log.info("Skipping tax sales (SKIP_SOURCES)")
        record_source_result("tax_sales", True, 0, "skipped")
        return []

    # Rockdale posts the current sale's property list as a document linked
    # from its tax-sales page -- find the newest link, then fetch it.
    try:
        links = await asyncio.to_thread(_tax_pdf_links, session)
        ranked = _rank_tax_pdfs(links)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not scan the Rockdale tax sales page: %s", exc)
        record_source_result("tax_sales", False, 0, str(exc))
        return []
    if not ranked:
        # Normal state between sales: the county takes the list down after
        # each auction and posts the next one ~4 weeks before the following
        # sale. Report ok, not failed.
        log.info("Rockdale tax sales page has no listing posted "
                 "(no active sale)")
        record_source_result("tax_sales", True, 0, "no active sale listing",
                             status="ok")
        return []

    name, url = ranked[0]
    log.info("Rockdale tax sale listing: %s (%s)", name, url)
    try:
        resp = await asyncio.to_thread(session.get, url, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not fetch the Rockdale tax sale listing: %s", exc)
        record_source_result("tax_sales", False, 0, str(exc))
        return []

    rows = await asyncio.to_thread(_parse_tax_sale_pdf, resp.content, url)
    if not rows:
        record_source_result("tax_sales", True, 0, "no active sales",
                             status="ok")
    else:
        record_source_result("tax_sales", True, len(rows))
    return rows




# SUPPLEMENTAL ADDRESS FILE (e.g. a PropStream export)
# =============================================================================
# PropStream has no public API, and driving its interface with a script would
# breach its terms the same way GSCCCA's would. What is fine is using data you
# already licensed: export from PropStream, drop the CSV at
# data/supplemental.csv, and it fills gaps the county parcel roll could not.
#
# Column names are matched loosely, so most exports work without editing.

SUPPLEMENTAL_PATH = DATA_DIR / "supplemental.csv"
SUPPLEMENTAL_XLSX = DATA_DIR / "supplemental.xlsx"
SKIPTRACE_IMPORT_PATHS = [DATA_DIR / "skiptrace_import.csv",
                          DASH_DIR / "skiptrace_import.csv"]

SUPP_HINTS = {
    "owner":        ["owner", "owner name", "owner 1", "owner1", "ownername",
                     "first name", "owner first name"],
    "owner_last":   ["owner last name", "last name", "owner 1 last name"],
    "prop_address": ["property address", "address", "site address", "situs",
                     "propertyaddress"],
    "prop_city":    ["property city", "city", "site city"],
    "prop_state":   ["property state", "state"],
    "prop_zip":     ["property zip", "zip", "zip code", "postal"],
    "mail_address": ["mailing address", "mail address", "owner address",
                     "mailingaddress"],
    "mail_city":    ["mailing city", "mail city", "owner city"],
    "mail_state":   ["mailing state", "mail state", "owner state"],
    "mail_zip":     ["mailing zip", "mail zip", "owner zip"],
    "parcel_id":    ["apn", "parcel", "parcel id", "parcel number", "pin"],
}


def _supp_column(header: str) -> Optional[str]:
    h = clean_text(header).lower().strip()
    for field, hints in SUPP_HINTS.items():
        for hint in hints:
            if h == hint:
                return field
    for field, hints in SUPP_HINTS.items():
        for hint in hints:
            if len(hint) >= 4 and hint in h:
                return field
    return None


class SupplementalIndex:
    """Optional second source of addresses, keyed by owner name and address."""

    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []
        self.by_name: Dict[str, Dict[str, Any]] = {}
        self.by_address: Dict[str, Dict[str, Any]] = {}
        self.by_parcel: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _rows_from_xlsx(path: Path) -> Tuple[List[str], List[Dict[str, Any]]]:
        """Read a spreadsheet export. PropStream hands out .xlsx, not .csv."""
        try:
            import openpyxl  # noqa: PLC0415
        except ImportError:
            log.warning("Found %s but openpyxl is not installed, so it cannot be "
                        "read. Either add openpyxl to requirements.txt or save "
                        "the file as CSV instead.", path.name)
            return [], []
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        it = ws.iter_rows(values_only=True)
        header = [clean_text(h) for h in next(it, ()) if h is not None]
        out = []
        for row in it:
            out.append({header[i]: row[i] for i in range(min(len(header), len(row)))})
        return header, out

    def load(self, path: Path = SUPPLEMENTAL_PATH) -> None:
        if not path.exists() and SUPPLEMENTAL_XLSX.exists():
            path = SUPPLEMENTAL_XLSX
        if not path.exists():
            return
        try:
            if path.suffix.lower() in (".xlsx", ".xlsm"):
                fieldnames, raw_rows = self._rows_from_xlsx(path)
                reader: Any = raw_rows
                reader_fieldnames = fieldnames
            else:
                text = path.read_text(encoding="utf-8-sig", errors="replace")
                dr = csv.DictReader(io.StringIO(text))
                reader = dr
                reader_fieldnames = dr.fieldnames or []
            colmap = {c: _supp_column(c) for c in reader_fieldnames}
            mapped = {v for v in colmap.values() if v}
            if not mapped:
                log.warning("Supplemental file has no columns I recognise: %s",
                            ", ".join(reader_fieldnames[:8]))
                return

            for raw in reader:
                rec: Dict[str, Any] = {}
                for col, field in colmap.items():
                    if not field:
                        continue
                    val = clean_text(raw.get(col))
                    if val and not rec.get(field):
                        rec[field] = val
                if rec.get("owner_last") and rec.get("owner"):
                    rec["owner"] = f"{rec['owner_last']} {rec['owner']}"
                if not (rec.get("owner") or rec.get("prop_address")):
                    continue
                self.rows.append(rec)

                pk = normalize_parcel_id(rec.get("parcel_id"))
                if pk:
                    self.by_parcel.setdefault(pk, rec)
                ak = address_key(rec.get("prop_address"), rec.get("prop_zip"))
                if ak:
                    self.by_address.setdefault(ak, rec)
                    bare = normalize_address(rec.get("prop_address"))
                    if bare:
                        self.by_address.setdefault(bare, rec)
                for key in name_variants(rec.get("owner", "")):
                    self.by_name.setdefault(key, rec)
                sig = token_signature(rec.get("owner", ""))
                if sig:
                    self.by_name.setdefault(sig, rec)

            log.info("Supplemental file loaded: %s rows from %s",
                     f"{len(self.rows):,}", path.name)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not read %s: %s", path.name, exc)

    def match(self, parcel_id: str = "", prop_address: str = "",
              owner: str = "") -> Optional[Dict[str, Any]]:
        pk = normalize_parcel_id(parcel_id)
        if pk and pk in self.by_parcel:
            return self.by_parcel[pk]
        for key in (address_key(prop_address), normalize_address(prop_address)):
            if key and key in self.by_address:
                return self.by_address[key]
        if owner:
            for key in (normalize_name(owner), token_signature(owner)):
                if key and key in self.by_name:
                    return self.by_name[key]
        return None


# =============================================================================
# SOURCE 5: QPUBLIC PARCEL ENRICHMENT (SchneiderCorp)
# =============================================================================
# Rockdale County's qPublic site (AppID=694) loads each parcel report as a plain
# GET with no login -- but sequential automated lookups can trip its
# ValidateUser.aspx bot challenge ("looks very similar to an automated
# request", reCAPTCHA "I'm not a robot"), observed 2026-10-02 on the Coweta
# build's second sequential load from a datacenter IP. Enrichment therefore
# stays polite (POLITE_DELAY between lookups, capped per run) and degrades
# gracefully: a challenged/blocked parcel is negative-cached with backoff and
# the record keeps whatever the source documents already said.
#
#   https://qpublic.schneidercorp.com/Application.aspx?AppID=694&LayerID=11394
#       &PageTypeID=4&PageID=4834&KeyValue=<PARCEL>
#
# Verified 2026-10-03: AppID=694/LayerID=11394 load Rockdale County GA pages,
# and the parcel-report PageID=4834 is confirmed by indexed Rockdale reports,
# e.g. KeyValue=045B010179 renders "Report: 045B010179". Like Coweta, the
# direct report URL works WITHOUT the Q= session parameter. Rockdale parcel
# ids are compact ("045B010022"), so the KeyValue needs no space encoding.
#
# The report carries the owner of record and the owner's mailing address --
# the two fields that power the absentee-owner flag and the CRM mailing
# columns. Plain HTTP clients hit a Cloudflare interstitial, so this runs in
# Playwright (real Chromium), the same browser the notices scraper uses.
#
# Best-effort by design: a parcel that will not load simply keeps whatever the
# source documents already said. Results are cached on disk because ownership
# rarely changes day to day, and lookups are capped per run as a courtesy.

QPUBLIC_URL = ("https://qpublic.schneidercorp.com/Application.aspx"
               "?AppID=694&LayerID=11394&PageTypeID=4&PageID=4834&KeyValue={key}")
QPUBLIC_CACHE_PATH = DATA_DIR / "qpublic_cache.json"
QPUBLIC_MAX_LOOKUPS = _int_env("QPUBLIC_MAX_LOOKUPS", 150)


def _qpublic_key(parcel_id: str) -> str:
    """'W09 030' -> 'W09+030'; '045B010022' -> '045B010022'.

    Each whitespace character becomes one '+'; whitespace is NOT collapsed,
    because the site renders the report title from the key verbatim
    ("W09       030" -> "W09+++++++030"). Rockdale parcel ids are compact
    (no spaces), so they pass through unchanged.
    """
    return re.sub(r"\s", "+", clean_text(parcel_id).strip())


def parse_qpublic_owner(html: str) -> Tuple[str, List[str]]:
    """
    Pull (owner_name, mailing_address_lines) out of a qPublic parcel report.
    Returns ("", []) when the owner block cannot be found.

    qPublic (SchneiderCorp Beacon) markup. Clayton-style template, verified
    2026-10-02 against Clayton County GA parcels 12238D A008 and 13107C C002
    (kept as a regression -- Rockdale uses the table template below)::

        <main id="maincontent">
          <section ...>                        <- ASP.NET id numbering varies;
                                                  select by the header text
            <header class="module-header">
              <div class="title">Owner</div>    <- exact text "Owner"
            </header>
            <div class="module-content">
              <div class="block-row">
                <div class="four-column-blocks">  <- first of three holds the data
                  <span id="..._sprLnkOwnerName1_..._lblSearch">NAME</span>
                  or <a id="..._sprLnkOwnerName1_..._lnkSearch">NAME</a>
                  <span id="..._sprLblOwnerName2_lblSuppressed"><br>EXTRA</span>
                  <span id="..._lblAddress1"><br>STREET</span>
                  <span id="..._lblAddress2"></span>
                  <span id="..._lblCityStZip"><br>CITY ST ZIP</span>
                </div>

    The ctlBodyPane_ctlNN_ id prefix is ASP.NET-generated and shifts, so
    elements are matched by id substring ("OwnerName1", "OwnerName2",
    "lblAddress1", "lblAddress2", "lblCityStZip"). <br> tags inside the
    spans are line breaks. Empty spans are present but hold no text.

    Rockdale County's Beacon template has NO four-column-blocks -- the Owner
    section is a table (same template family as Coweta's, verified 2026-10-02
    on Coweta parcels W09 030 and 111 1019 052; Rockdale's Owner section
    renders name/street/city-state-zip in one cell the same way, verified
    2026-10-03 from the crawled report for parcel 045B010022):

        <div class="module-content">
          <table class="tabular-data-two-column" ...>
            <tr><th scope="row">
              <span id="..._lnkOwnerName_lblSearch">LEAPHART PAMELA JOY</span>
              <span id="..._lblAddress"><br>1584 CHERRY HIL CT SW</span>
              <span id="..._lblCityStateZip"><br>CONYERS, GA 30094</span>
            </th>...

    Rockdale's city/state/ZIP line carries a comma ("CONYERS, GA 30094").
    """
    soup = BeautifulSoup(html, "lxml")
    owner_section = None
    for section in soup.find_all("section"):
        title = section.find("div", class_="title")
        if title is not None and clean_text(title.get_text()) == "Owner":
            owner_section = section
            break
    if owner_section is None:
        return "", []
    content = owner_section.find("div", class_="module-content")
    if content is None:
        return "", []
    blocks = content.find_all("div", class_="four-column-blocks")
    block = blocks[0] if blocks else content

    def find_by_id_part(part: str):
        return block.find(id=lambda v: v is not None and part in v)

    def lines_of(el) -> List[str]:
        if el is None:
            return []
        for br in el.find_all("br"):
            br.replace_with("\n")
        return [ln.strip() for ln in el.get_text().split("\n") if ln.strip()]

    name_lines = lines_of(find_by_id_part("OwnerName1")) \
        + lines_of(find_by_id_part("OwnerName2"))
    if name_lines:
        owner = ", ".join(name_lines)
        mail_lines = (lines_of(find_by_id_part("lblAddress1"))
                      + lines_of(find_by_id_part("lblAddress2"))
                      + lines_of(find_by_id_part("lblCityStZip")))
        return owner, mail_lines

    # Rockdale table template: name / street / city-state-zip as three spans
    # inside the Owner section's table. Match by id SUFFIX so the Clayton
    # "lblAddress1"/"lblAddress2" spans are never picked up here.
    def find_by_id_suffix(suffix: str):
        return content.find(id=lambda v: v is not None and v.endswith(suffix))

    rockdale_name = lines_of(find_by_id_suffix("lnkOwnerName_lblSearch"))
    if not rockdale_name:
        return "", []
    owner = ", ".join(rockdale_name)
    mail_lines = (lines_of(find_by_id_suffix("_lblAddress"))
                  + lines_of(find_by_id_suffix("_lblCityStateZip")))
    return owner, mail_lines


def load_qpublic_cache() -> Dict[str, Dict[str, Any]]:
    blob = safe_read_json(QPUBLIC_CACHE_PATH, {}) or {}
    return blob if isinstance(blob, dict) else {}


def _qpublic_note_failure(cache: Dict[str, Dict[str, Any]], pid: str) -> None:
    """
    Negative-cache a failed parcel lookup with exponential backoff (2, 4, 8
    ... days, capped at 30). A parcel that never loads stops burning the
    daily lookup budget after its first failure instead of every morning.
    """
    prior = cache.get(pid) or {}
    attempts = int(prior.get("attempts") or 0) + 1
    backoff = min(2 ** min(attempts, 5), 30)
    cache[pid] = {
        "failed": True,
        "attempts": attempts,
        "next_retry": (now_et() + timedelta(days=backoff)).strftime("%Y-%m-%d"),
        "fetched": today_et(),
    }


def _qpublic_retry_due(entry: Dict[str, Any]) -> bool:
    """A negatively cached parcel becomes eligible again after next_retry."""
    return bool(entry.get("failed")) and \
        (entry.get("next_retry") or "") <= today_et()


async def enrich_from_qpublic(records: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Fill parcel_owner + mailing address for records that carry a parcel_id,
    using the county's qPublic report pages. Returns (enriched, failed).
    """
    from playwright.async_api import async_playwright  # local import: optional dep

    want = {}
    for rec in records:
        pid = clean_text(rec.get("parcel_id", ""))
        if not pid or rec.get("mail_address"):
            continue
        want.setdefault(pid, []).append(rec)
    if not want:
        return 0, 0

    cache = load_qpublic_cache()
    fresh = {pid: rs for pid, rs in want.items()
             if pid not in cache or _qpublic_retry_due(cache[pid])}
    if fresh:
        log.info("qPublic: %d parcels to look up (%d cached)",
                 len(fresh), len(want) - len(fresh))

    enriched = failed = 0
    if fresh:
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=HEADLESS)
                try:
                    page = await browser.new_page()
                    for i, (pid, recs) in enumerate(
                            list(fresh.items())[:QPUBLIC_MAX_LOOKUPS]):
                        try:
                            url = QPUBLIC_URL.format(key=_qpublic_key(pid))
                            await page.goto(url, timeout=45000)
                            await page.wait_for_timeout(2500)
                            html = await page.content()
                            if ("validateuser.aspx" in page.url.lower()
                                    or "i'm not a robot" in html.lower()):
                                # qPublic bot challenge -- observed on Rockdale
                                # 2026-10-02 after sequential lookups from a
                                # datacenter IP. Stop the run's lookups here
                                # instead of hammering the challenge page.
                                log.warning("qPublic bot challenge on %s; "
                                            "stopping enrichment this run", pid)
                                _qpublic_note_failure(cache, pid)
                                failed += 1
                                break
                            owner, mail_lines = parse_qpublic_owner(html)
                            if not owner:
                                failed += 1
                                _qpublic_note_failure(cache, pid)
                                log.debug("qPublic: no owner block for %s", pid)
                                continue
                            entry = {"owner": owner, "mail": mail_lines,
                                     "fetched": today_et()}
                            cache[pid] = entry
                            enriched += _apply_qpublic_entry(recs, entry)
                        except Exception as exc:  # noqa: BLE001
                            failed += 1
                            _qpublic_note_failure(cache, pid)
                            if failed <= 3:
                                log.warning("qPublic lookup failed for %s: %s",
                                            pid, exc)
                            else:
                                log.debug("qPublic lookup failed for %s: %s",
                                          pid, exc)
                        if i < len(fresh) - 1:
                            await page.wait_for_timeout(int(POLITE_DELAY * 1000))
                finally:
                    await browser.close()
        except Exception as exc:  # noqa: BLE001
            log.warning("qPublic enrichment unavailable this run: %s", exc)
            failed += len(fresh)

    # Records whose parcel was cached on an earlier run still get filled in.
    for pid, recs in want.items():
        if pid in cache and pid not in fresh and not cache[pid].get("failed"):
            enriched += _apply_qpublic_entry(recs, cache[pid])

    if fresh:
        safe_write_json(QPUBLIC_CACHE_PATH, cache)
    log.info("qPublic enrichment: %d enriched, %d failed", enriched, failed)
    return enriched, failed


def apply_qpublic_cache(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Cache-only pass over records that are NOT new this run: fill mailing
    addresses from earlier qPublic lookups without spending the lookup
    budget. Returns the records that gained a mailing address they did not
    have -- previously exported leads that got better, and go out in
    updated_leads.csv.
    """
    cache = load_qpublic_cache()
    updated = []
    for rec in records:
        if rec.get("mail_address"):
            continue
        pid = clean_text(rec.get("parcel_id", ""))
        entry = cache.get(pid)
        if not entry or entry.get("failed"):
            continue
        _apply_qpublic_entry([rec], entry)
        if rec.get("mail_address"):
            updated.append(rec)
    if updated:
        log.info("qPublic cache gave %d previously exported records a mailing "
                 "address", len(updated))
    return updated


def _apply_qpublic_entry(recs: List[Dict[str, Any]],
                         entry: Dict[str, Any]) -> int:
    """Stamp one cache entry onto every record sharing the parcel. Returns count."""
    mail = entry.get("mail") or []
    n = 0
    for rec in recs:
        if not rec.get("parcel_owner"):
            rec["parcel_owner"] = entry.get("owner", "")
        if mail and not rec.get("mail_address"):
            rec["mail_address"] = mail[0] if len(mail) > 0 else ""
            # Last line is usually "CITY ST ZIP"; middle lines are street cont.
            if len(mail) >= 2:
                street_extra, city, state, zipc = _split_city_state_zip(mail[-1])
                rec["mail_city"] = city
                rec["mail_state"] = state
                rec["mail_zip"] = zipc
                street_bits = [b for b in mail[:-1] if b]
                if street_extra:
                    street_bits.append(street_extra)
                if street_bits:
                    rec["mail_address"] = ", ".join(street_bits)
        rec["match_method"] = "qpublic"
        rec["match_confidence"] = max(rec.get("match_confidence") or 0, 0.95)
        rec["owner_occupied"] = determine_owner_occupancy(rec)
        n += 1
    return n


def _split_city_state_zip(line: str) -> Tuple[str, str, str, str]:
    """
    'ELLENWOOD GA 30294' -> ('', 'Ellenwood', 'GA', '30294').
    ZIP+4 works hyphenated or space-separated:
    'ELLENWOOD GA 30294-2213' -> ('', 'Ellenwood', 'GA', '30294-2213').
    'ELLENWOOD GA 30294 2213' -> ('', 'Ellenwood', 'GA', '30294-2213').
    A PO box jammed onto the city line comes back as street_extra:
    'PO BOX 1234 ATLANTA GA 30303' -> ('PO Box 1234', 'Atlanta', 'GA', '30303').
    """
    m = re.match(r"^(.*?)\s+([A-Z]{2})\s+(\d{5}(?:[-\s]?\d{4})?)\s*$",
                 clean_text(line).upper())
    if m:
        # Rockdale's qPublic renders "CONYERS, GA 30094" -- strip the comma (or
        # stray period) the state abbreviation leaves behind.
        city = m.group(1).rstrip(",.").title()
        state, zipc = m.group(2), m.group(3).replace(" ", "-")
        pm = re.match(r"^(P\.?\s*O\.?\s*BOX\s+[\w-]+)\s+(.*)$", city, re.I)
        if pm:
            return pm.group(1).strip(), pm.group(2).strip() or city, state, zipc
        return "", city, state, zipc
    return "", clean_text(line), "", ""


# =============================================================================
# ENRICHMENT: attach parcel data
# =============================================================================

def enrich_with_parcels(records: List[Dict[str, Any]], parcels: ParcelIndex,
                        supplemental: Optional["SupplementalIndex"] = None
                        ) -> Tuple[int, int]:
    matched = unmatched = 0
    from_supp = 0
    for rec in records:
        try:
            parcel, confidence, method = parcels.match(
                parcel_id=rec.get("parcel_id", ""),
                prop_address=rec.get("prop_address", ""),
                owner=rec.get("owner", ""),
                legal=rec.get("legal", ""),
            )
            rec["match_confidence"] = round(confidence, 2)
            rec["match_method"] = method

            if not parcel and supplemental is not None:
                # The county roll did not know this one; try the file you
                # exported from PropStream before giving up on it.
                alt = supplemental.match(parcel_id=rec.get("parcel_id", ""),
                                         prop_address=rec.get("prop_address", ""),
                                         owner=rec.get("owner", ""))
                if alt:
                    from_supp += 1
                    matched += 1
                    for field in ("prop_address", "prop_city", "prop_state",
                                  "prop_zip", "mail_address", "mail_city",
                                  "mail_state", "mail_zip", "parcel_id"):
                        if alt.get(field) and not rec.get(field):
                            rec[field] = alt[field]
                    rec["match_confidence"] = max(rec.get("match_confidence") or 0, 0.7)
                    rec["match_method"] = "supplemental file"
                    rec["owner_occupied"] = determine_owner_occupancy(rec)
                    continue

            if not parcel:
                unmatched += 1
                rec.setdefault("prop_city", "")
                rec.setdefault("prop_state", STATE_ABBR)
                rec.setdefault("prop_zip", rec.get("prop_zip", ""))
                rec.setdefault("mail_address", "")
                rec.setdefault("mail_city", "")
                rec.setdefault("mail_state", "")
                rec.setdefault("mail_zip", "")
                rec.setdefault("owner_occupied", None)
                continue

            matched += 1
            site = ParcelIndex.site_address(parcel)
            rec["parcel_id"] = clean_text(parcel.get("PARCELID")) or rec.get("parcel_id", "")
            rec["prop_address"] = site or rec.get("prop_address", "")
            rec["prop_city"] = clean_text(parcel.get("CITY"))
            rec["prop_state"] = clean_text(parcel.get("STATE")) or STATE_ABBR
            rec["prop_zip"] = clean_text(parcel.get("ZIP")) or rec.get("prop_zip", "")

            rec["mail_address"] = clean_text(parcel.get("PSTLADDRESS"))
            rec["mail_city"] = clean_text(parcel.get("PSTLCITY"))
            rec["mail_state"] = clean_text(parcel.get("PSTLSTATE"))
            rec["mail_zip"] = clean_text(parcel.get("PSTLZIP5"))

            rec["parcel_owner"] = clean_text(parcel.get("OWNERNME1"))
            rec["assessed_value"] = parcel.get("TOTAPR1")
            rec["use_description"] = clean_text(parcel.get("USEDSCRP"))
            if not rec.get("legal"):
                rec["legal"] = clean_text(parcel.get("PRPRTYDSCRP"))

            rec["owner_occupied"] = determine_owner_occupancy(rec)
        except Exception as exc:  # noqa: BLE001
            unmatched += 1
            log.debug("Enrichment failed for %s: %s", rec.get("doc_num"), exc)
    if from_supp:
        log.info("  %d of those came from the supplemental file", from_supp)
    return matched, unmatched


def determine_owner_occupancy(rec: Dict[str, Any]) -> Optional[bool]:
    prop = normalize_address(rec.get("prop_address"))
    mail = normalize_address(rec.get("mail_address"))
    if not prop or not mail:
        return None
    if is_po_box(rec.get("mail_address")):
        return False
    if prop == mail:
        return True
    # A mailing address that starts with the same house number + street is the
    # same property with a formatting difference, not an absentee owner.
    if prop.split() and mail.startswith(" ".join(prop.split()[:2])):
        return True
    return False


# =============================================================================
# DEDUPLICATION + PROPERTY CONSOLIDATION + RELEASE HANDLING
# =============================================================================

def dedupe_records(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """Exact-document dedupe on source + doc_num (or the notice dedupe hash)."""
    seen: Dict[str, Dict[str, Any]] = {}
    dupes = 0
    for rec in records:
        key = rec.get("_notice_dedupe") or f"{rec.get('source','')}|{rec.get('doc_num','')}"
        key = key.strip().upper()
        if key in seen:
            dupes += 1
            prior = seen[key]
            # Newspaper ads run four consecutive weeks. Keep the earliest filing
            # date but remember that we saw it again today.
            pf, cf = parse_date(prior.get("filed")), parse_date(rec.get("filed"))
            if pf and cf and cf < pf:
                prior["filed"] = rec["filed"]
            prior["last_verified"] = fmt_date(utcnow().replace(tzinfo=None))
            if not prior.get("amount") and rec.get("amount"):
                prior["amount"] = rec["amount"]
            if not prior.get("prop_address") and rec.get("prop_address"):
                prior["prop_address"] = rec["prop_address"]
            continue
        rec["last_verified"] = fmt_date(utcnow().replace(tzinfo=None))
        seen[key] = rec
    return list(seen.values()), dupes


def property_key(rec: Dict[str, Any]) -> str:
    """Property identity, in priority order: parcel id, address, owner+legal."""
    pid = normalize_parcel_id(rec.get("parcel_id"))
    if pid:
        return f"PID:{pid}"
    addr = address_key(rec.get("prop_address"), rec.get("prop_zip"))
    if addr:
        return f"ADR:{addr}"
    sig = token_signature(rec.get("owner"))
    legal = normalize_address(rec.get("legal"))[:60]
    if sig:
        return f"OWN:{sig}|{legal}"
    return f"DOC:{rec.get('source','')}|{rec.get('doc_num','')}"


def apply_release_handling(records: List[Dict[str, Any]]) -> int:
    """
    A recorded release cancels the distress signal it points at. Without this
    the list fills up with lis pendens that were resolved months ago.
    """
    by_property: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in records:
        by_property[property_key(rec)].append(rec)

    released = 0
    for group in by_property.values():
        releases = [r for r in group if r.get("is_release") or r.get("cat") == "RELLP"]
        if not releases:
            continue
        for rel in releases:
            rel_dt = parse_date(rel.get("filed"))
            for other in group:
                if other is rel or other.get("is_release"):
                    continue
                if other.get("cat") not in DISTRESS_CATEGORIES:
                    continue
                other_dt = parse_date(other.get("filed"))
                # Only a release filed at/after the distress document counts.
                if rel_dt and other_dt and rel_dt < other_dt:
                    continue
                if rel.get("cat") == "RELLP" and other.get("cat") != "LP":
                    continue
                other["status"] = "released"
                other["released_by"] = rel.get("doc_num")
                released += 1
    return released


def consolidate_flags(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Build per-property context. John Smith's judgment (Monday), lis pendens
    (Wednesday) and foreclosure ad (Thursday) are one motivated seller with
    three stacked signals -- not three separate leads.
    """
    context: Dict[str, Dict[str, Any]] = {}
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in records:
        grouped[property_key(rec)].append(rec)

    for key, group in grouped.items():
        active = [r for r in group if r.get("status") != "released"]
        cats = {r.get("cat") for r in active if r.get("cat") in DISTRESS_CATEGORIES}
        amounts = [r["amount"] for r in active if isinstance(r.get("amount"), (int, float))]
        context[key] = {
            "categories": cats,
            "doc_count": len(group),
            "active_count": len(active),
            "max_amount": max(amounts) if amounts else None,
            "has_lp_and_fc": {"LP", "FC"}.issubset(cats),
            "distinct_distress": len(cats),
        }
    return context


# =============================================================================
# FLAGS + SELLER SCORE
# =============================================================================

# Property already held by a government body is not a lead. The city cannot
# sell it to you, and a tax-commissioner grantee on a fifa is normal and should
# not be confused with the owner.
GOVERNMENT_OWNER_RE = re.compile(
    r"\b(?:CITY\s+OF|COUNTY\s+OF|STATE\s+OF\s+GEORGIA|"
    r"FULTON\s+COUNTY|GWINNETT\s+COUNTY|ROCKDALE\s+COUNTY|COWETA\s+COUNTY|COBB\s+COUNTY|"
    r"HOUSING\s+AUTHORITY|BOARD\s+OF\s+EDUCATION|SCHOOL\s+DISTRICT|"
    r"DEPARTMENT\s+OF\s+TRANSPORTATION|DEPT\s+OF\s+TRANSPORTATION|"
    r"UNITED\s+STATES|U\.?S\.?A\.?\b|SECRETARY\s+OF\s+HOUSING|"
    r"URBAN\s+DEVELOPMENT|\bMARTA\b|WATER\s+AUTHORITY|"
    r"DEVELOPMENT\s+AUTHORITY|LAND\s+BANK|TAX\s+COMMISSIONER|"
    r"MUNICIPAL|GEORGIA\s+POWER|REGIONAL\s+COMMISSION)\b", re.I)


def is_government_owner(name: Any) -> bool:
    return bool(GOVERNMENT_OWNER_RE.search(clean_text(name).upper()))


CORP_TOKENS = {"LLC", "INC", "CORP", "CORPORATION", "LP", "LLP", "COMPANY",
               "HOLDINGS", "PROPERTIES", "INVESTMENTS"}


def build_flags(rec: Dict[str, Any], ctx: Dict[str, Any],
                start: datetime, end: datetime) -> List[str]:
    flags: List[str] = []

    # Distress categories present anywhere on this property, deduplicated.
    for cat in sorted(ctx.get("categories", set())):
        flag = CAT_FLAGS.get(cat)
        if flag and flag not in flags:
            flags.append(flag)

    if rec.get("cat") == "TAX":
        # A sale date in the past is not an upcoming auction -- it is a done
        # deal in its redemption period, which is a different conversation.
        sale_dt = parse_date(rec.get("tax_sale_date")
                             or rec.get("foreclosure_sale_date") or "")
        if sale_dt and sale_dt.date() < now_et().date():
            if "Past tax sale / redemption period" not in flags:
                flags.append("Past tax sale / redemption period")
        elif (rec.get("tax_sale_date") or rec.get("foreclosure_sale_date")) \
                and "Tax sale" not in flags:
            flags.append("Tax sale")

    owner_tokens = set(name_tokens(rec.get("owner", "")))
    if owner_tokens & CORP_TOKENS:
        flags.append("LLC / corp owner")

    if rec.get("owner_occupied") is False:
        flags.append("Absentee owner")

    if ctx.get("distinct_distress", 0) >= 2:
        flags.append("Multiple distress signals")

    filed = parse_date(rec.get("filed"))
    if filed and start.date() <= filed.date() <= end.date():
        flags.append("New this week")

    if rec.get("heirs_unknown"):
        flags.append("Unknown heirs / unsettled estate")

    if rec.get("status") == "released":
        flags.append("Released / resolved")

    # Preserve order, drop duplicates.
    out, seen = [], set()
    for f in flags:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


MAJOR_DISTRESS_FLAGS = {
    "Lis pendens", "Pre-foreclosure", "Judgment lien", "Tax lien", "Tax delinquent",
    "Tax sale", "Mechanic lien", "HOA lien", "Government lien", "Probate / estate", "Lien",
}

# Not every distress signal means the same thing, and a flat +10 for each made a
# foreclosure score the same as a roofer's lien. These weights rank by how
# likely the owner is to actually sell, and how soon.
#
# They matter more here than the original rubric assumed, because DeKalb's
# search results carry no dollar figures -- the amount lives inside the
# document image -- so the amount bonuses almost never fire on clerk records.
# Without weighting by type there is nothing left to rank on.
CATEGORY_WEIGHT = {
    "FC": 30,       # sale already scheduled; the hardest deadline there is
    "PRO": 25,      # inherited a house, often out of area and wanting out
    "LP": 25,       # suit filed against the property
    "TAX": 25,      # headed for the courthouse steps
    "TAXLIEN": 20,  # IRS or state revenue; rarely resolved quietly
    "JUD": 15,      # fi fa on the general execution docket
    "MECH": 15,     # contractor unpaid, usually mid-project and out of money
    "MED": 12,
    "HOA": 12,
    "LIEN": 10,     # unspecified lien
    "NOC": 0,       # building work starting: investing, not leaving
    "RELLP": 0,
}

FLAG_TO_CATEGORY = {v: k for k, v in CAT_FLAGS.items()}


def score_record(rec: Dict[str, Any], ctx: Dict[str, Any], flags: List[str]) -> int:
    score = 30

    # Weight each distinct distress category by seriousness rather than counting
    # them all the same. Categories come from the property context, so signals
    # stacked across several documents on one house all count.
    cats = set(ctx.get("categories") or ())
    if not cats and rec.get("cat"):
        cats = {rec["cat"]}
    for cat in cats:
        score += CATEGORY_WEIGHT.get(cat, 10)

    # Flags that carry weight but are not categories.
    if "Tax sale" in flags or "Past tax sale / redemption period" in flags:
        score += 5

    if ctx.get("has_lp_and_fc"):
        score += 10
    if ctx.get("distinct_distress", 0) >= 3:
        score += 10

    amount = ctx.get("max_amount") or rec.get("amount")
    if isinstance(amount, (int, float)):
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10

    if "New this week" in flags:
        score += 5
    if rec.get("prop_address"):
        score += 5
    # An owner who does not live there has already left. For an investor that
    # is one of the better predictors on the list, so it is worth more than the
    # original +5.
    if "Absentee owner" in flags:
        score += 6
    # A verified match is worth more than a guessed one.
    conf = rec.get("match_confidence") or 0
    if conf >= 0.9:
        score += 3
    elif conf >= 0.75:
        score += 2

    # A released lien is not a motivated seller.
    if rec.get("status") == "released":
        score = int(score * 0.4)
    # A notice of commencement means somebody is investing in the property.
    if rec.get("cat") == "NOC":
        score = min(score, 45)
    # A release is evidence a problem went away. It exists to downgrade the
    # document it cancels, not to become a lead in its own right.
    if rec.get("cat") == "RELLP" or rec.get("is_release"):
        score = min(score, 25)
    # Low-confidence matches should not outrank verified ones.
    if rec.get("match_confidence", 0) and rec["match_confidence"] < 0.6:
        score -= 5

    return max(0, min(100, score))


# =============================================================================
# OUTPUT
# =============================================================================

OUTPUT_FIELDS = [
    "doc_num", "doc_type", "filed", "cat", "cat_label", "owner", "parcel_owner",
    "name_order", "grantee",
    "amount", "legal", "parcel_id", "prop_address", "prop_city", "prop_state",
    "prop_zip", "pub_city", "mail_address", "mail_city", "mail_state", "mail_zip",
    "owner_occupied", "clerk_url", "source", "foreclosure_sale_date",
    "tax_sale_date", "years_delinquent", "heirs_unknown",
    "notice_number", "status", "match_confidence", "match_method",
    "last_verified", "flags", "score", "county", "first_seen",
]


def shape_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for field in OUTPUT_FIELDS:
        value = rec.get(field)
        if field == "prop_state":
            value = value or STATE_ABBR
        if field == "county":
            value = value or COUNTY
        if field in ("amount", "score", "owner_occupied", "match_confidence"):
            out[field] = value
        elif field == "flags":
            out[field] = value or []
        else:
            out[field] = "" if value is None else value
    return out


def existing_lead_count() -> int:
    """How many leads are already published, if any."""
    for path in RECORDS_JSON_PATHS:
        blob = safe_read_json(path)
        if isinstance(blob, dict) and isinstance(blob.get("records"), list):
            if blob["records"]:
                return len(blob["records"])
    return 0


def archive_key(rec: Dict[str, Any]) -> str:
    """Stable identity for a document across runs and across counties."""
    county = rec.get("county") or COUNTY
    doc = clean_text(rec.get("doc_num"))
    if doc:
        return f"{county}|{rec.get('source','')}|{doc}"
    # A few notice sources publish no document number; fall back to content.
    return f"{county}|" + sha_key(rec.get("source"), rec.get("owner"),
                                  rec.get("filed"), rec.get("doc_type"),
                                  rec.get("prop_address"))


def merge_archive(shaped: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Fold this run into the running archive and hand back everything we hold.

    Each run only scrapes a few days back, so a file that is overwritten every
    morning can never answer "show me the last 30 days". The archive is the
    memory: first sighting of a document sets first_seen and that date never
    moves, while the rest of the row is refreshed so a later run's better
    address or score wins.
    """
    prior = safe_read_json(ARCHIVE_PATH, []) or []
    if not isinstance(prior, list):
        log.warning("Archive at %s was not a list -- starting a fresh one.", ARCHIVE_PATH.name)
        prior = []

    merged: Dict[str, Dict[str, Any]] = {}
    for rec in prior:
        if isinstance(rec, dict):
            merged[archive_key(rec)] = rec

    today = today_et()
    added = updated = 0
    for rec in shaped:
        key = archive_key(rec)
        existing = merged.get(key)
        if existing:
            rec["first_seen"] = existing.get("first_seen") or rec.get("filed") or today
            if rec.get("score") is None:
                # A re-seen document skips the scoring pass when NEW_ONLY is on,
                # so it arrives with no score and no flags. Letting it overwrite
                # the archived row would erase the score it earned before --
                # keep the earlier values instead.
                rec["score"] = existing.get("score")
                if not rec.get("flags"):
                    rec["flags"] = existing.get("flags") or []
            updated += 1
        else:
            rec["first_seen"] = rec.get("first_seen") or today
            added += 1
        merged[key] = rec

    cutoff = (now_et() - timedelta(days=ARCHIVE_DAYS)).strftime("%Y-%m-%d")

    def keep(rec: Dict[str, Any]) -> bool:
        stamp = rec.get("first_seen") or rec.get("filed") or ""
        return not stamp or stamp >= cutoff

    out = [r for r in merged.values() if keep(r)]
    dropped = len(merged) - len(out)
    out.sort(key=lambda r: (-(r.get("score") or 0), _filed_sort_key(r.get("filed"))))

    safe_write_json(ARCHIVE_PATH, out)
    log.info("Archive: %d new, %d refreshed, %d retired, %d held in total",
             added, updated, dropped, len(out))
    return out


def write_outputs(records: List[Dict[str, Any]], start: datetime, end: datetime,
                  archive_input: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Publish the dashboard file.

    `records` is what the CSV exports care about (today's fresh finds when
    NEW_ONLY is on). `archive_input` is everything this run saw, fresh or not,
    which is what gets folded into the archive so history survives.
    """
    shaped_all = [shape_record(r) for r in (archive_input
                                           if archive_input is not None else records)]
    for rec in shaped_all:
        rec.setdefault("first_seen", "")

    # Never let a bad morning erase a good list. If every source failed, the
    # archive still holds yesterday's leads, so publish those rather than an
    # empty page.
    archive = merge_archive(shaped_all) if shaped_all else (
        safe_read_json(ARCHIVE_PATH, []) or [])

    if not archive:
        prior = existing_lead_count()
        if prior:
            log.warning("No records collected this run and no archive yet -- keeping "
                        "the %d leads already published.", prior)
            for path in RECORDS_JSON_PATHS:
                blob = safe_read_json(path)
                if isinstance(blob, dict):
                    blob["last_attempt_at"] = now_et().isoformat()
                    blob["last_attempt_status"] = "no records collected; showing previous run"
                    blob["sources_report"] = SOURCE_REPORT
                    safe_write_json(path, blob)
            return safe_read_json(RECORDS_JSON_PATHS[0], {}) or {}

    cutoff = (now_et() - timedelta(days=DASHBOARD_DAYS)).strftime("%Y-%m-%d")
    published = [r for r in archive
                 if (r.get("first_seen") or r.get("filed") or "") >= cutoff] or archive

    stamps = [r.get("first_seen") for r in archive if r.get("first_seen")]
    filed_dates = [r.get("filed") for r in archive if r.get("filed")]

    payload = {
        "fetched_at": now_et().isoformat(),
        "source": f"{COUNTY} County {STATE_ABBR} Public Records",
        "date_range": {"start": fmt_date(start), "end": fmt_date(end)},
        "run_window": {"start": fmt_date(start), "end": fmt_date(end)},
        "archive_range": {"start": min(stamps) if stamps else "",
                          "end": max(stamps) if stamps else ""},
        "filed_range": {"start": min(filed_dates) if filed_dates else "",
                        "end": max(filed_dates) if filed_dates else ""},
        "new_this_run": len(records),
        "archive_total": len(archive),
        "dashboard_days": DASHBOARD_DAYS,
        "total": len(published),
        "with_address": sum(1 for r in published if r.get("prop_address")),
        "sources_report": SOURCE_REPORT,
        "records": published,
    }

    for path in RECORDS_JSON_PATHS:
        safe_write_json(path, payload)
        log.info("Wrote %s (%d records on the dashboard, %d in the archive)",
                 path.relative_to(REPO_ROOT), len(published), len(archive))
    return payload


def _filed_sort_key(value: Any) -> str:
    """
    Sort helper for descending dates via an ascending sort. Inverting each digit
    (9 - d) turns "2026-08-25" into a string that ascends as the date descends,
    and blanks map to the largest key so undated rows land last.
    """
    text = str(value or "")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        return "~~~~~~~~~~"
    return "".join(str(9 - int(c)) if c.isdigit() else c for c in text)


def collapse_to_properties(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    One row per property for CRM import. Three documents on the Tate house is one
    motivated seller, not three contacts -- importing it three times means calling
    the same person three times. The highest-scoring document represents the
    property; its flag list already carries every signal found across all of them,
    and the union of document types is preserved for context.
    """
    best: Dict[str, Dict[str, Any]] = {}
    extra_types: Dict[str, List[str]] = defaultdict(list)
    for rec in records:
        key = property_key(rec)
        dt = rec.get("doc_type", "")
        if dt and dt not in extra_types[key]:
            extra_types[key].append(dt)
        cur = best.get(key)
        if cur is None or (rec.get("score") or 0) > (cur.get("score") or 0):
            best[key] = rec

    out = []
    for key, rec in best.items():
        merged = dict(rec)
        types = extra_types.get(key, [])
        if len(types) > 1:
            merged["doc_type"] = " | ".join(types[:4])
        out.append(merged)
    out.sort(key=lambda r: -(r.get("score") or 0))
    log.info("GHL export collapsed %d documents to %d unique properties",
             len(records), len(out))
    return out


def collapse_to_contacts(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Guarantee one row per human being.

    collapse_to_properties() already reduced many documents to one row per
    property, but a single owner can be distressed on several properties at
    once, which would put their name in the CSV more than once and create
    duplicate contacts on import. Here the same person (same normalized name at
    the same mailing address) is reduced to a single row: their highest-scoring
    property becomes the contact's property, and the fact that they own several
    distressed properties is preserved as a flag -- it is a strong buying
    signal, not noise.
    """
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in records:
        sig = token_signature(rec.get("owner")) or normalize_name(rec.get("owner"))
        mail = normalize_address(rec.get("mail_address"))
        if not sig:
            # No usable name: keep the row on its own so nothing is silently lost.
            sig = f"UNNAMED:{rec.get('doc_num', '')}"
        groups[f"{sig}|{mail}"].append(rec)

    out: List[Dict[str, Any]] = []
    merged_away = 0
    for group in groups.values():
        group.sort(key=lambda r: -(r.get("score") or 0))
        primary = dict(group[0])
        if len(group) > 1:
            merged_away += len(group) - 1
            flags = list(primary.get("flags") or [])
            note = f"Owns {len(group)} distressed properties"
            if note not in flags:
                flags.append(note)
            primary["flags"] = flags
            # Owning several distressed properties is itself a motivation signal.
            primary["score"] = min(100, (primary.get("score") or 0) + 5)
        out.append(primary)

    out.sort(key=lambda r: -(r.get("score") or 0))
    if merged_away:
        log.info("GHL export merged %d extra rows so each owner appears exactly once",
                 merged_away)
    return out


def _write_csv_paths(blob: str, targets: List[Path], encoding: str) -> None:
    for path in targets:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".csv.tmp")
        tmp.write_text(blob, encoding=encoding)
        os.replace(tmp, path)


def _dated_export_path(kind: str) -> Path:
    """data/exports/ghl_2026-10-02.csv -- one snapshot per run, so a missed
    day never requires git archaeology to reconstruct."""
    d = DATA_DIR / "exports"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{kind}_{today_et()}.csv"


def export_ghl_csv(records: List[Dict[str, Any]],
                   paths: Optional[List[Path]] = None) -> None:
    columns = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL", "Tags",
    ]
    targets = paths if paths is not None else GHL_CSV_PATHS
    main_export = paths is None

    # Always collapse. One seller must never appear twice in the CRM import.
    records = collapse_to_contacts(collapse_to_properties(records))

    if not records:
        if main_export and NEW_ONLY:
            # Nothing new today: write an honest header-only file rather than
            # leaving yesterday's export in place pretending it is today's.
            # The dated snapshot keeps the day in history either way.
            buf = io.StringIO()
            csv.DictWriter(buf, fieldnames=columns).writeheader()
            _write_csv_paths(buf.getvalue(), targets, "utf-8-sig")
            _dated_export_path("ghl").write_text(buf.getvalue(),
                                                 encoding="utf-8-sig")
            log.info("No new records today -- wrote header-only ghl_leads.csv")
            return
        existing = [p for p in targets if p.exists() and p.stat().st_size > 200]
        if existing:
            log.warning("No records this run -- leaving the existing %s in place",
                        existing[0].name)
            return

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    written = 0
    for rec in records:
        try:
            # The parcel roll's owner of record beats the document's owner
            # spelling when we have it; it is always LAST FIRST order.
            owner = rec.get("parcel_owner") or rec.get("owner", "")
            order = ("last-first" if rec.get("parcel_owner")
                     else rec.get("name_order") or "last-first")
            first, last = split_person_name(owner, order)
            amount = rec.get("amount")
            writer.writerow({
                "First Name": first,
                "Last Name": last,
                "Mailing Address": rec.get("mail_address", ""),
                "Mailing City": rec.get("mail_city", ""),
                "Mailing State": rec.get("mail_state", ""),
                "Mailing Zip": rec.get("mail_zip", ""),
                "Property Address": street_only(rec.get("prop_address"),
                                               rec.get("prop_city"),
                                               rec.get("prop_state"),
                                               rec.get("prop_zip")),
                "Property City": rec.get("prop_city", ""),
                "Property State": rec.get("prop_state", STATE_ABBR),
                "Property Zip": rec.get("prop_zip", ""),
                "Lead Type": rec.get("cat_label", ""),
                "Document Type": rec.get("doc_type", ""),
                "Date Filed": rec.get("filed", ""),
                "Document Number": rec.get("doc_num", ""),
                "Amount/Debt Owed": f"{amount:.2f}" if isinstance(amount, (int, float)) else "",
                "Seller Score": rec.get("score", 0),
                "Motivated Seller Flags": "; ".join(rec.get("flags", []) or []),
                "Source": rec.get("source", ""),
                "Public Records URL": rec.get("clerk_url", ""),
                "Tags": f"{COUNTY_SCRAPER_TAG},{crm_type_tag(rec)}",
            })
            written += 1
        except Exception as exc:  # noqa: BLE001
            log.debug("CSV row skipped for %s: %s", rec.get("doc_num"), exc)

    blob = buf.getvalue()

    # Final guarantee, checked against the bytes actually being written.
    names = [r.split(",")[0] + "|" + r.split(",")[1]
             for r in blob.splitlines()[1:] if r.count(",") > 2]
    dupes = len(names) - len(set(names))
    if dupes:
        # Two different people can share a name, and they are kept apart on
        # purpose -- merging them would put one person's lien on another
        # person's house. This is a note, not a fault.
        log.info("CSV: %d rows. %d share a name with another lead but sit at "
                 "different addresses, so they are kept separate.", written, dupes)
    else:
        log.info("CSV verified: %d rows, every owner appears exactly once", written)

    _write_csv_paths(blob, targets, "utf-8-sig")
    for path in targets:
        log.info("Wrote %s (%d rows)", path.relative_to(REPO_ROOT), written)
    if main_export:
        dated = _dated_export_path("ghl")
        dated.write_text(blob, encoding="utf-8-sig")
        log.info("Wrote %s", dated.relative_to(REPO_ROOT))


def write_skiptrace_import(records: List[Dict[str, Any]]) -> None:
    """
    Write every lead with a property address in the skip tracer's template:

        Address,City,State,Zip,Tag
        989 Montreal Road E,Clarkston,GA,30021,Pre-Foreclosure

    The Tag carries the lead type, so once a deal closes you can see what
    brought it in without going back to look up which list it came from.

    One row per property. Where several distress signals sit on the same house,
    the tag names the strongest of them -- a foreclosure with a lien on it
    should read as a foreclosure, not as whichever document was filed last.
    """
    best: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        street = clean_text(rec.get("prop_address"))
        if not street:
            continue
        key = address_key(street, rec.get("prop_zip")) or normalize_address(street)
        if not key:
            continue
        cur = best.get(key)
        if cur is None:
            best[key] = rec
            continue
        # Prefer the more serious category; fall back to the better score.
        w_new = CATEGORY_WEIGHT.get(rec.get("cat", ""), 0)
        w_cur = CATEGORY_WEIGHT.get(cur.get("cat", ""), 0)
        if w_new > w_cur or (w_new == w_cur and
                             (rec.get("score") or 0) > (cur.get("score") or 0)):
            best[key] = rec

    rows = sorted(best.values(), key=lambda r: -(r.get("score") or 0))
    if not rows:
        if NEW_ONLY:
            # Nothing new today: header-only, so the file never masquerades
            # as a fresh list.
            buf = io.StringIO()
            csv.writer(buf).writerow(["Address", "City", "State", "Zip", "Tag"])
            _write_csv_paths(buf.getvalue(), SKIPTRACE_IMPORT_PATHS, "utf-8")
            _dated_export_path("skiptrace").write_text(buf.getvalue(),
                                                       encoding="utf-8")
            log.info("No addresses to skip trace this run -- wrote header-only file")
            return
        log.info("No addresses to skip trace this run")
        return

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Address", "City", "State", "Zip", "Tag"])
    tags: Dict[str, int] = defaultdict(int)
    for rec in rows:
        tag = clean_text(rec.get("cat_label")) or "Motivated Seller"
        tags[tag] += 1
        writer.writerow([
            street_only(rec.get("prop_address"), rec.get("prop_city"),
                        rec.get("prop_state"), rec.get("prop_zip")),
            clean_text(rec.get("prop_city")),
            clean_text(rec.get("prop_state")) or STATE_ABBR,
            clean_text(rec.get("prop_zip"))[:5],
            tag,
        ])

    blob = buf.getvalue()
    _write_csv_paths(blob, SKIPTRACE_IMPORT_PATHS, "utf-8")
    for path in SKIPTRACE_IMPORT_PATHS:
        log.info("Wrote %s (%d addresses ready to skip trace)",
                 path.relative_to(REPO_ROOT), len(rows))
    dated = _dated_export_path("skiptrace")
    dated.write_text(blob, encoding="utf-8")
    log.info("Wrote %s", dated.relative_to(REPO_ROOT))
    for tag, n in sorted(tags.items(), key=lambda kv: -kv[1]):
        log.info("    %-32s %d", tag, n)


def push_to_gohighlevel(records: List[Dict[str, Any]]) -> None:
    """
    Optional. Runs only when both env vars are set; the CSV export never depends
    on it. Kept deliberately small so the auth/endpoint details can be filled in
    against whichever GHL API version the account is on.
    """
    api_key = os.getenv("GHL_API_KEY", "").strip()
    location_id = os.getenv("GHL_LOCATION_ID", "").strip()
    if not (api_key and location_id):
        log.info("GHL push skipped (GHL_API_KEY / GHL_LOCATION_ID not set)")
        return

    session = build_session()
    session.headers.update({
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Version": os.getenv("GHL_API_VERSION", "2021-07-28"),
    })
    endpoint = os.getenv("GHL_CONTACTS_ENDPOINT", "https://services.leadconnectorhq.com/contacts/")
    pushed = failed = 0
    for rec in records:
        if (rec.get("score") or 0) < int(os.getenv("GHL_MIN_SCORE", "60")):
            continue
        first, last = split_person_name(rec.get("owner", ""),
                                        rec.get("name_order") or "last-first")
        body = {
            "locationId": location_id,
            "firstName": first, "lastName": last,
            "address1": rec.get("mail_address", ""),
            "city": rec.get("mail_city", ""),
            "state": rec.get("mail_state", ""),
            "postalCode": rec.get("mail_zip", ""),
            "source": rec.get("source", ""),
            "tags": [COUNTY_SCRAPER_TAG, crm_type_tag(rec)] + [
                f.lower().replace(" ", "-") for f in (rec.get("flags") or [])],
            "customFields": [
                {"key": "property_address", "field_value": rec.get("prop_address", "")},
                {"key": "seller_score", "field_value": str(rec.get("score", 0))},
                {"key": "public_records_url", "field_value": rec.get("clerk_url", "")},
            ],
        }
        try:
            resp = session.post(endpoint, json=body, timeout=HTTP_TIMEOUT)
            if resp.status_code < 300:
                pushed += 1
            else:
                failed += 1
                log.debug("GHL rejected %s: %s %s", rec.get("doc_num"),
                          resp.status_code, resp.text[:160])
        except Exception as exc:  # noqa: BLE001
            failed += 1
            log.debug("GHL push error for %s: %s", rec.get("doc_num"), exc)
        time.sleep(0.25)
    log.info("GHL push complete: %d contacts, %d failures", pushed, failed)


# =============================================================================
# SEEN-HISTORY (so "New this week" means filed recently, not first-seen today)
# =============================================================================

def load_seen() -> Dict[str, str]:
    blob = safe_read_json(SEEN_STATE_PATH, {})
    return blob if isinstance(blob, dict) else {}


def save_seen(records: List[Dict[str, Any]], prior: Dict[str, str]) -> None:
    today = today_et()
    for rec in records:
        key = rec.get("_notice_dedupe") or f"{rec.get('source','')}|{rec.get('doc_num','')}"
        prior.setdefault(key, rec.get("filed") or today)
    # Trim anything older than a year so the state file cannot grow forever.
    cutoff = (now_et() - timedelta(days=365)).strftime("%Y-%m-%d")
    trimmed = {k: v for k, v in prior.items() if (v or "9999") >= cutoff}
    safe_write_json(SEEN_STATE_PATH, trimmed)


# =============================================================================
# MANUAL NAME IMPORT
# =============================================================================
# A bridge for when a source site is uncooperative. You copy owner names off
# Georgia Public Notice by hand -- something that takes a few minutes and that
# you already know how to do -- and everything downstream still runs: parcel
# matching, mailing addresses, absentee detection, scoring, dashboard, CSV.
#
# One name per line in a text file. Anything after a pipe is optional:
#
#     ALICIA NICOLE KENNON | FC | 90000
#     TATE TYRONE          | FC
#     WESLEY CHAPEL VENTURES LLC
#
# Field 2 is the lead type (FC, LP, TAX, JUD, PRO, LIEN...) and defaults to FC.
# Field 3 is the amount owed, if the notice states one.

def load_manual_names(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        log.error("No such file: %s", path)
        return []

    out: List[Dict[str, Any]] = []
    today = utcnow().replace(tzinfo=None)
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split("|")]
        owner = parts[0]
        if not owner:
            continue
        cat = (parts[1].upper() if len(parts) > 1 and parts[1] else "FC")
        if cat not in CAT_LABELS:
            log.warning("  line %d: unknown lead type %r, using FC", lineno, cat)
            cat = "FC"
        amount = parse_money(parts[2]) if len(parts) > 2 else None

        out.append({
            "doc_num": f"MANUAL-{sha_key(owner, cat)}",
            "doc_type": "Notice of Sale Under Power" if cat == "FC"
                        else CAT_LABELS.get(cat, cat),
            "filed": fmt_date(today),
            "cat": cat,
            "cat_label": CAT_LABELS.get(cat, cat),
            "owner": owner,
            "grantee": "",
            "amount": amount,
            "legal": "",
            "parcel_id": "",
            "prop_address": "",
            "clerk_url": LEGAL_NOTICE_SEARCH_URL,
            "source": "Manually collected from Georgia Public Notice",
            "foreclosure_sale_date": None,
            "notice_number": "",
            "status": "active",
            "is_release": False,
        })
    log.info("Read %d names from %s", len(out), path.name)
    return out


def run_from_names(path: Path) -> int:
    """Turn a list of owner names into a finished, scored lead file."""
    t0 = time.time()
    end = now_et().replace(tzinfo=None)
    start = end - timedelta(days=LOOKBACK_DAYS)

    log.info("=" * 74)
    log.info("Building leads from names in %s", path)
    log.info("=" * 74)

    records = load_manual_names(path)
    if not records:
        log.error("Nothing to do -- the file had no usable names.")
        return 1

    parcels = ParcelIndex()
    parcels.load(build_session())
    if not parcels.parcels:
        log.warning("No parcel data; continuing with source-document addresses only.")

    matched, unmatched = enrich_with_parcels(records, parcels)
    log.info("Matched to a property: %d of %d", matched, len(records))

    context = consolidate_flags(records)
    for rec in records:
        ctx = context.get(property_key(rec), {})
        rec["flags"] = build_flags(rec, ctx, start, end)
        rec["score"] = score_record(rec, ctx, rec["flags"])

    payload = write_outputs(records, start, end)
    export_ghl_csv([shape_record(r) for r in records])

    log.info("=" * 74)
    log.info("%d leads | %d with a mailing address", payload["total"],
             sum(1 for r in payload["records"] if r.get("mail_address")))
    for rec in sorted(records, key=lambda r: -(r.get("score") or 0))[:10]:
        log.info("  %3d  %-28s %-26s %s", rec.get("score", 0),
                 (rec.get("owner") or "")[:28],
                 (rec.get("prop_address") or "no match")[:26],
                 ", ".join(rec.get("flags", [])[:3]))
    log.info("Wrote data/ghl_leads.csv -- ready to import")
    log.info("Done in %.1fs", time.time() - t0)
    log.info("=" * 74)
    if unmatched:
        log.info("%d name(s) did not match a Rockdale parcel. Usually a spelling "
                 "difference, a trust, or a property outside the county.", unmatched)
    return 0


# =============================================================================
# MAIN
# =============================================================================

async def run_all() -> int:
    t0 = time.time()
    # Eastern, not UTC: the cron fires at 11:47 UTC, and a UTC clock has already
    # rolled into tomorrow on any run after 8pm in Georgia.
    end = now_et().replace(tzinfo=None)
    start = end - timedelta(days=LOOKBACK_DAYS)

    log.info("=" * 74)
    log.info("%s County %s motivated seller scraper", COUNTY, STATE_ABBR)
    log.info("Started %s ET | window %s .. %s (%d days)",
             end.strftime("%Y-%m-%d %H:%M"), fmt_date(start), fmt_date(end), LOOKBACK_DAYS)
    log.info("Headless=%s  GSCCCA premium=%s", HEADLESS, GSCCCA_PREMIUM)
    log.info("=" * 74)

    session = build_session()

    # --- 1. Parcels first: everything else enriches against this ------------
    parcels = ParcelIndex()
    try:
        parcels.load(session)
    except Exception as exc:  # noqa: BLE001
        log.error("Parcel load failed entirely: %s", exc)
        record_source_result("arcgis_parcels", False, 0, str(exc))

    # --- 2. Record sources (independent; one failing never stops the rest) ---
    all_records: List[Dict[str, Any]] = []

    clerk_records = await scrape_gsccca(start, end)
    log.info("GSCCCA records found: %d", len(clerk_records))
    all_records.extend(clerk_records)

    notice_records = await scrape_legal_notices(start, end, session)
    log.info("Foreclosure/legal notices found: %d", len(notice_records))
    all_records.extend(notice_records)

    tax_records = await scrape_tax_sales(session)
    log.info("Tax delinquent/tax sale records found: %d", len(tax_records))
    all_records.extend(tax_records)

    if not all_records:
        log.warning("No records collected from any source this run. "
                    "Existing output files are left untouched.")

    # --- 3a. Drop anything a government body already owns --------------------
    before = len(all_records)
    all_records = [r for r in all_records if not is_government_owner(r.get("owner"))]
    gov_dropped = before - len(all_records)
    if gov_dropped:
        log.info("Dropped %d record(s) owned by a city, county or agency -- "
                 "nothing to buy there", gov_dropped)

    # --- 3. Dedupe ----------------------------------------------------------
    all_records, dupes = dedupe_records(all_records)
    log.info("Duplicates collapsed: %d  |  unique documents: %d", dupes, len(all_records))

    # --- 4. Enrich ----------------------------------------------------------
    supplemental = SupplementalIndex()
    supplemental.load()

    matched, unmatched = enrich_with_parcels(all_records, parcels, supplemental)
    log.info("Parcel matches: %d matched, %d unmatched", matched, unmatched)

    # A parcel can change hands to the county between filings, so check again
    # now that every record carries the roll's own idea of who owns it.
    before = len(all_records)
    all_records = [r for r in all_records
                   if not is_government_owner(r.get("parcel_owner"))]
    if before - len(all_records):
        log.info("Dropped %d more now the parcel roll shows a government owner",
                 before - len(all_records))

    # --- 4b. NEW_ONLY selection BEFORE qPublic enrichment -------------------
    # The lookup budget goes to new records first; previously seen records
    # only ever consult the cache, so a backlog of dead parcels can never
    # starve today's leads.
    everything_seen = list(all_records)
    prior_seen = load_seen()

    def _key(r: Dict[str, Any]) -> str:
        return (r.get("_notice_dedupe")
                or f"{r.get('source','')}|{r.get('doc_num','')}")

    fresh = list(all_records)
    if NEW_ONLY and prior_seen:
        fresh = [r for r in all_records if _key(r) not in prior_seen]
        log.info("New since the last run: %d of %d documents",
                 len(fresh), len(all_records))
        if not fresh:
            log.info("Nothing new today -- CSVs carry the full archived list")

    # qPublic parcel reports: owner of record + mailing address, looked up by
    # parcel number. New records get live lookups; previously exported records
    # only consult the cache. Best-effort; failures leave the record as-is.
    updated: List[Dict[str, Any]] = []
    if "PARCELS" not in SKIP_SOURCES and not PARCEL_LAYER:
        try:
            qp_enriched, qp_failed = await enrich_from_qpublic(fresh)
            qp_ok = qp_enriched > 0 or qp_failed == 0
            SOURCE_REPORT["qpublic"] = {
                "ok": qp_ok, "status": "ok" if qp_ok else "failed",
                "count": qp_enriched,
                "error": f"{qp_failed} lookups failed" if qp_failed else "",
            }
        except Exception as exc:  # noqa: BLE001
            log.warning("qPublic enrichment skipped: %s", exc)
            SOURCE_REPORT["qpublic"] = {"ok": False, "status": "failed", "count": 0,
                                        "error": f"qpublic: {exc}"[:120]}
        if prior_seen:
            fresh_keys = {_key(r) for r in fresh}
            updated = apply_qpublic_cache(
                [r for r in everything_seen if _key(r) not in fresh_keys])

    # --- 5. Releases, consolidation, flags, score ---------------------------
    released = apply_release_handling(all_records)
    if released:
        log.info("Distress records downgraded by a matching release: %d", released)

    context = consolidate_flags(all_records)
    log.info("Distinct properties represented: %d", len(context))

    # Score the fresh records (and any previously exported ones that just
    # gained a mailing address); everything else keeps its earlier score.
    for rec in fresh + updated:
        try:
            ctx = context.get(property_key(rec), {})
            rec["flags"] = build_flags(rec, ctx, start, end)
            rec["score"] = score_record(rec, ctx, rec["flags"])
        except Exception as exc:  # noqa: BLE001
            log.debug("Scoring failed for %s: %s", rec.get("doc_num"), exc)
            rec.setdefault("flags", [])
            rec.setdefault("score", 30)

    # --- 6. Output ----------------------------------------------------------
    # CSV exports carry the FULL rolling lead list (Rell, 2026-10-05):
    # new-only exports left him with blank attachments most days. new_this_run
    # is still tracked in the payload for the email body. The archive still
    # gets everything this run saw, or history develops holes on any day a
    # document is re-seen rather than newly found.
    payload = write_outputs(fresh, start, end, archive_input=everything_seen)
    full_records = payload.get("records", [])
    export_ghl_csv(full_records)
    write_skiptrace_import(full_records)
    push_to_gohighlevel([shape_record(r) for r in fresh])
    if updated:
        export_ghl_csv([shape_record(r) for r in updated],
                       paths=UPDATED_CSV_PATHS)
        log.info("Wrote updated_leads.csv: %d previously exported records "
                 "gained a mailing address", len(updated))

    save_seen(everything_seen, prior_seen)
    if UNMAPPED_DOC_TYPES:
        prior = set(safe_read_json(UNKNOWN_DOCTYPES_PATH, []) or [])
        combined = sorted(prior | UNMAPPED_DOC_TYPES)
        safe_write_json(UNKNOWN_DOCTYPES_PATH, combined)
        log.info("Unmapped document types logged: %d (see %s)",
                 len(UNMAPPED_DOC_TYPES), UNKNOWN_DOCTYPES_PATH.name)

    # --- 7. Report ----------------------------------------------------------
    recs = payload.get("records", [])
    scored = [r for r in recs if (r.get("score") or 0) >= 60]
    log.info("=" * 74)
    log.info("FINAL: %d leads | %d with property address | %d scoring 60+",
             payload["total"], payload["with_address"], len(scored))
    if recs:
        bands = [("80-100 very high", 80, 101), ("60-79  high", 60, 80),
                 ("40-59  moderate", 40, 60), ("under 40 lower", 0, 40)]
        for label, lo, hi in bands:
            n = sum(1 for r in recs if lo <= (r.get("score") or 0) < hi)
            log.info("    %-18s %5d", label, n)
        with_amt = sum(1 for r in recs if isinstance(r.get("amount"), (int, float)))
        log.info("    records carrying a dollar amount: %d", with_amt)
        if with_amt == 0:
            log.info("    (the clerk's results grid has no amount column -- the "
                     "figure lives inside the document image, so ranking is by "
                     "distress type instead)")
    for name, info in SOURCE_REPORT.items():
        mark = {"ok": "OK  ", "skipped": "SKIP", "failed": "FAIL"}.get(
            info.get("status"), "????")
        log.info("  [%s] %-18s %5d records %s", mark, name, info["count"],
                 f"-- {info['error']}" if info["error"] else "")
    log.info("Execution time: %.1fs", time.time() - t0)
    log.info("=" * 74)

    # The "browser" entry only says Chromium launched; it is not a data
    # source. Skipped sources (no premium account, nothing configured) are
    # not failures either. Fail only when every enabled data source failed.
    data_sources = {n: i for n, i in SOURCE_REPORT.items() if n != "browser"}
    enabled = {n: i for n, i in data_sources.items()
               if i.get("status", "failed") != "skipped"}
    if enabled and all(i.get("status") != "ok" for i in enabled.values()):
        log.error("Every enabled data source failed (%s). Exiting non-zero "
                  "so the Action surfaces it.", ", ".join(sorted(enabled)))
        return 1
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Rockdale County GA motivated seller scraper")
    ap.add_argument("--lookback", type=int, help="override LOOKBACK_DAYS")
    ap.add_argument("--headful", action="store_true", help="run browsers visibly")
    ap.add_argument("--gsccca-verify", action="store_true",
                    help="probe GSCCCA premium search controls and exit")
    ap.add_argument("--skip", default="", help="comma list: GSCCCA,NOTICES,TAX,PARCELS")
    ap.add_argument("--from-names", metavar="FILE",
                    help="build leads from a text file of owner names instead "
                         "of scraping (one name per line)")
    args = ap.parse_args()

    global LOOKBACK_DAYS, HEADLESS, SKIP_SOURCES, GSCCCA_VERIFY
    if args.lookback:
        LOOKBACK_DAYS = args.lookback
    if args.headful:
        HEADLESS = False
    if args.gsccca_verify:
        GSCCCA_VERIFY = True
    if args.skip:
        SKIP_SOURCES |= {s.strip().upper() for s in args.skip.split(",") if s.strip()}

    if args.from_names:
        try:
            return run_from_names(Path(args.from_names))
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed: %s", exc)
            return 1

    # Debug log for the diagnostics artifact. *.log is gitignored, so this is
    # never committed -- the workflow uploads it when a run needs diagnosing.
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        _fh = logging.FileHandler(DATA_DIR / "debug.log", encoding="utf-8")
        _fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-7s %(name)-14s %(message)s", "%H:%M:%S"))
        logging.getLogger().addHandler(_fh)
    except Exception:  # noqa: BLE001
        pass

    try:
        return asyncio.run(run_all())
    except KeyboardInterrupt:
        log.warning("Interrupted by user")
        return 130
    except Exception as exc:  # noqa: BLE001
        log.exception("Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

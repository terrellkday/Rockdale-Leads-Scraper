"""Regression tests for the 2026-10-02 Claude-review fixes.

Run:  python3 scraper/test_regression.py
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fetch import (  # noqa: E402
    _qpublic_key,
    _split_city_state_zip,
    _strip_notice_header,
    _tax_pdf_rank,
    _looks_like_tax_listing,
    _rank_tax_pdfs,
    _rockdale_sale_date,
    _clean_rockdale_owner,
    build_flags,
    categorize,
    parse_money,
    sha_key,
    split_person_name,
    _parse_tax_sale_pdf,
    PARCEL_ID_IN_TEXT_RE,
    LegalNoticeScraper,
)


def test_zip_plus4_with_space():
    # The qPublic mailing line "ELLENWOOD GA 30294 2213" used to come back
    # with the whole line as the city and empty state/ZIP.
    assert _split_city_state_zip("ELLENWOOD GA 30294 2213") == (
        "", "Ellenwood", "GA", "30294-2213")
    # Hyphenated form keeps working and keeps the +4.
    assert _split_city_state_zip("ELLENWOOD GA 30294-2213") == (
        "", "Ellenwood", "GA", "30294-2213")
    # Plain ZIP untouched.
    assert _split_city_state_zip("ELLENWOOD GA 30294") == (
        "", "Ellenwood", "GA", "30294")


def test_notice_id_from_ad_code():
    body = ("CND7862 GPN11 NOTICE OF FORECLOSURE OF RIGHT TO REDEEM REAL "
            "PROPERTY FROM TAX SALE. County: Coweta. TO: Marcia Davis and "
            "Kitoshia Eason. Pursuant to O.C.G.A. 48-4-45, the property will "
            "be sold at public outcry before the courthouse door to the "
            "highest bidder for cash. This is a legal advertisement.")
    html = (f"<html><body><div class='results'><div class='ad'>"
            f"<div class='adbody'>{body}</div></div></div></body></html>")
    assert len(body) > 150
    out = LegalNoticeScraper._parse_results(html, "TAX")
    # Nested divs must not double-emit: one ad, one record.
    assert len(out) == 1, f"expected 1 record, got {len(out)}"
    # The leading alphanumeric ad code is the stable id, not a content hash
    # (fixture code kept from the Clayton-era regression suite).
    assert out[0]["notice_id"] == "CND7862", out[0]["notice_id"]


def test_notice_hash_stable_across_republication():
    t1 = ("NOTICE OF SALE UNDER POWER Wednesday, October 1, 2025 "
          "County: Coweta body body body")
    t2 = ("NOTICE OF SALE UNDER POWER Wednesday, October 8, 2025 "
          "County: Coweta body body body")
    assert sha_key(_strip_notice_header(t1)) == sha_key(_strip_notice_header(t2))
    # ...but genuinely different notices still hash differently.
    t3 = ("NOTICE OF SALE UNDER POWER Wednesday, October 8, 2025 "
          "County: Coweta different body here")
    assert sha_key(_strip_notice_header(t1)) != sha_key(_strip_notice_header(t3))


def test_split_person_name_orders():
    # Legal notices use natural order.
    assert split_person_name("Marcia Davis and Kitoshia Eason", "natural") == (
        "Marcia", "Davis")
    assert split_person_name("OLGA MARIA VEGA", "natural") == (
        "Olga Maria", "Vega")
    # Joint owners on &: first person only.
    assert split_person_name("DAVIS MARCIA & EASON KITOSHIA", "last-first") == (
        "Marcia", "Davis")
    # Deed-index order is the default.
    assert split_person_name("THOMAS IZOIA P") == ("Izoia P", "Thomas")
    assert split_person_name("SMITH JOHN", "last-first") == ("John", "Smith")


def test_tax_pdf_rank_two_digit_year():
    assert _tax_pdf_rank("NOVEMBER TAX SALE LISTING 9-8-26.pdf")[:2] == (2026, 11)
    assert _tax_pdf_rank("10-6-26.pdf")[:2] == (2026, 10)
    assert _tax_pdf_rank("september_2026_tax_sale.pdf")[:2] == (2026, 9)


def test_tax_listing_name_filter():
    assert _looks_like_tax_listing("september_2026_tax_sale.pdf")
    assert _looks_like_tax_listing("NOVEMBER TAX SALE LISTING 9-8-26.pdf")
    assert _looks_like_tax_listing("TAX SALE LISTING-APRIL 2026-1.pdf")
    assert not _looks_like_tax_listing("DQ759GA.pdf")
    assert not _looks_like_tax_listing("DQ759GA_20250204.pdf")
    assert not _looks_like_tax_listing("budget_2027.pdf")
    assert not _looks_like_tax_listing("Tax_Real_Property_Return.pdf")
    assert not _looks_like_tax_listing("OfficialClaimforExcessFund.pdf")


def test_tax_pdf_rank_prefers_listing_date():
    links = [
        ("september_2026_tax_sale.pdf", "u1", "2026-08-06"),
        ("NOVEMBER TAX SALE LISTING 9-8-26.pdf", "u2", "2026-09-09"),
        ("DQ759GA.pdf", "u3", "2026-09-10"),
    ]
    ranked = _rank_tax_pdfs(links)
    assert [n for n, _ in ranked] == [
        "NOVEMBER TAX SALE LISTING 9-8-26.pdf",
        "september_2026_tax_sale.pdf",
    ]


def test_past_tax_sale_flag():
    start = datetime.now() - timedelta(days=3)
    end = datetime.now()
    past = {"cat": "TAX", "tax_sale_date": "2026-09-01", "owner": "X"}
    flags = build_flags(past, {"categories": set()}, start, end)
    assert "Past tax sale / redemption period" in flags, flags
    assert "Tax sale" not in flags, flags
    future = {"cat": "TAX", "tax_sale_date": "2026-11-03", "owner": "X"}
    flags2 = build_flags(future, {"categories": set()}, start, end)
    assert "Tax sale" in flags2, flags2
    assert "Past tax sale / redemption period" not in flags2, flags2


def test_parse_money_fallback_capped():
    # A huge digit run with no $ sign is not a $100B debt.
    assert parse_money("ref 99999999999x") is None
    assert parse_money("$12,196.37") == 12196.37


def test_categorize_word_boundary():
    assert categorize("TAX SALE")[0] == "TAX"
    # "ESTATE" must not match inside a longer word.
    assert categorize("REALESTATE HOLDINGS")[0] == "UNK"


def test_split_city_state_zip_comma():
    # Coweta's qPublic renders "NEWNAN, GA 30265" -- the comma must not end
    # up in the city name.
    assert _split_city_state_zip("NEWNAN, GA 30265") == (
        "", "Newnan", "GA", "30265")
    assert _split_city_state_zip("SHARPSBURG, GA 30277") == (
        "", "Sharpsburg", "GA", "30277")


def test_parcel_id_coweta_formats():
    # Coweta's alphanumeric parcels plus the Clayton-era numeric pattern.
    assert PARCEL_ID_IN_TEXT_RE.search("parcel W09 030 here").group(1) == "W09 030"
    assert PARCEL_ID_IN_TEXT_RE.search("parcel 111 1019 052 here").group(1) == "111 1019 052"
    assert PARCEL_ID_IN_TEXT_RE.search("parcel 05 079 02 003 here").group(1) == "05 079 02 003"


def test_parcel_id_rockdale_formats():
    # Rockdale's compact parcels: 9-10 bare digits, or the letter-bearing form.
    for pid in ["0690010241", "0180040001", "045B010022", "087A010050",
                "041001022B", "066001025A", "014001008C", "093A01079A",
                "020001018K", "080B010227", "032A010003", "C380010164",
                "C070010002", "C010030012", "0630250008", "0300010261"]:
        m = PARCEL_ID_IN_TEXT_RE.search(f"parcel {pid} here")
        assert m and m.group(1) == pid, pid
    # An 8-digit YYYYMMDD date must not match the bare-digit form.
    assert not PARCEL_ID_IN_TEXT_RE.search("dated 20261002 here")
    # A 12-digit run must not match either.
    assert not PARCEL_ID_IN_TEXT_RE.search("ref 123456789012 x")


def test_qpublic_key_preserves_spacing():
    # Each whitespace char becomes one '+'; nothing is collapsed.
    assert _qpublic_key("W09 030") == "W09+030"
    assert _qpublic_key("111 1019 052") == "111+1019+052"
    # Rockdale's compact parcel ids have no spaces: pass through unchanged.
    assert _qpublic_key("045B010022") == "045B010022"
    assert _qpublic_key("0690010241") == "0690010241"


def test_rockdale_tax_sale_date_from_title():
    # The sale date comes from the list's own title.
    assert _rockdale_sale_date("ROCKDALE COUNTY TAX SALE LIST APRIL 1, 2025") == "2025-04-01"
    assert _rockdale_sale_date("MAY 4, 2021 TAX SALE PROPERTIES (AS OF 5/03/2021)") == "2021-05-04"
    assert _rockdale_sale_date("no date here") == ""


def test_rockdale_owner_cleanup():
    # ", IN REM" and the heirs tail are not part of the owner's name.
    assert _clean_rockdale_owner("URBAN PROPERTY SOLUTIONS LLC, IN REM") == \
        "URBAN PROPERTY SOLUTIONS LLC"
    assert _clean_rockdale_owner(
        "MENSINGER ANNIE LOIS, IN REM, ALL HEIRS KNOWN & UNKNOWN") == \
        "MENSINGER ANNIE LOIS"
    # Dollar amounts that leak into the owner column are debris, not name.
    assert _clean_rockdale_owner("SMITH JOHN A $ 2,100.00") == "SMITH JOHN A"


def _rockdale_list_pdf():
    # Synthetic Rockdale tax-sale list: FILE # | YEARS | PARCEL | OWNER |
    # OPENING BID, matching the county's published layout. Built with
    # PyMuPDF (a hard scraper dependency) so the positional parser is
    # tested end to end.
    import pymupdf
    doc = pymupdf.open()
    page = doc.new_page(width=700, height=842)
    y = 60
    page.insert_text((60, y), "ROCKDALE COUNTY TAX SALE LIST OCTOBER 6, 2026",
                     fontsize=14)
    y += 30
    for x, h in [(60, "FILE #"), (130, "YEARS"), (210, "PARCEL"),
                 (320, "OWNER"), (560, "OPENING BID")]:
        page.insert_text((x, y), h, fontsize=10)
    y += 20
    rows = [
        ("337", "2019", "032A010003",
         "URBAN PROPERTY SOLUTIONS LLC, IN REM", "$ 2,357.68"),
        ("1", "2023", "0690010241", "", "$ 2,681.98"),
        ("58", "2018-2019", "080B010227",
         "BRYSON FRANKLIN L, IN REM", "$ 1,236.53"),
        ("102", "2021", "C380010164", "SMITH JOHN A", "$2,100.00"),
        ("215", "2020,2021", "093A01079A",
         "JONES MARY K, ALL HEIRS KNOWN & UNKNOWN", "$ 5,000.00"),
    ]
    for f, yrs, pid, owner, bid in rows:
        page.insert_text((60, y), f, fontsize=10)
        page.insert_text((130, y), yrs, fontsize=10)
        page.insert_text((210, y), pid, fontsize=10)
        page.insert_text((320, y), owner, fontsize=10)
        page.insert_text((560, y), bid, fontsize=10)
        y += 18
    return doc.tobytes()


def test_rockdale_tax_pdf_layout():
    # The full Rockdale list layout parses to clean rows: parcel-anchored,
    # owners without legal tails, amounts and sale date intact.
    out = _parse_tax_sale_pdf(
        _rockdale_list_pdf(), "https://rockdaletaxoffice.org/property-tax-sales")
    assert len(out) == 5, f"expected 5 rows, got {len(out)}"
    first = out[0]
    assert first["parcel_id"] == "032A010003"
    assert first["owner"] == "URBAN PROPERTY SOLUTIONS LLC"
    assert first["amount"] == 2357.68
    assert first["filed"] == "2026-10-06"
    assert first["years_delinquent"] == "2019"
    assert out[1]["parcel_id"] == "0690010241" and out[1]["owner"] == ""
    assert out[2]["owner"] == "BRYSON FRANKLIN L"
    assert out[2]["years_delinquent"] == "2018-2019"
    assert out[3]["parcel_id"] == "C380010164" and out[3]["amount"] == 2100.0
    assert out[4]["owner"] == "JONES MARY K"
    assert out[4]["years_delinquent"] == "2020,2021"
    assert out[4]["doc_num"] == "TAX-202610-093A01079A"


if __name__ == "__main__":
    test_zip_plus4_with_space()
    test_notice_id_from_ad_code()
    test_notice_hash_stable_across_republication()
    test_split_person_name_orders()
    test_tax_pdf_rank_two_digit_year()
    test_tax_listing_name_filter()
    test_tax_pdf_rank_prefers_listing_date()
    test_past_tax_sale_flag()
    test_parse_money_fallback_capped()
    test_categorize_word_boundary()
    test_split_city_state_zip_comma()
    test_parcel_id_coweta_formats()
    test_parcel_id_rockdale_formats()
    test_qpublic_key_preserves_spacing()
    test_rockdale_tax_sale_date_from_title()
    test_rockdale_owner_cleanup()
    test_rockdale_tax_pdf_layout()
    print("ALL REGRESSION TESTS PASSED")

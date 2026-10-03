"""Parser tests for parse_qpublic_owner in scraper/fetch.py.

Fixtures are verbatim markup captured 2026-10-02 from live qPublic
(SchneiderCorp Beacon) parcel report pages for Clayton County GA:
  - 12238D A008 : owner name renders as a <span>, OwnerName2 carries a
                  second owner line after a <br>
  - 13107C C002 : owner name renders as an <a> (__doPostBack href),
                  OwnerName2 is empty
Run:  python3 scraper/test_qpublic_parser.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch import parse_qpublic_owner

TPL = """<html><body><main id="maincontent">
<section id="ctlBodyPane_ctl05_mSection" class="avoid-page-break">
<header id="ctlBodyPane_ctl05_divHeader" class="module-header" moduleid="45861">
<a class="toggle-collapse"><div id="ctlBodyPane_ctl05_lblName" class="title" role="heading" aria-level="2">Owner</div></a>
</header>
<div class="module-content">
<div class="block-row">
<div class="four-column-blocks">
<span id="ctlBodyPane_ctl05_ctl01_sprLblOwnerTitle_lblSuppressed"></span>
{BODY}
<span id="ctlBodyPane_ctl05_ctl01_lblAddress1"><br>{A1}</span>
<span id="ctlBodyPane_ctl05_ctl01_lblAddress2">{A2}</span>
<span id="ctlBodyPane_ctl05_ctl01_lblCityStZip"><br>{CSZ}</span>
</div>
<div class="four-column-blocks"><span id="ctlBodyPane_ctl05_ctl01_sprLblOwnerMultiple_lblSuppressed"></span></div>
<div class="four-column-blocks"><span id="ctlBodyPane_ctl05_ctl01_sprLblJanuaryOwner_lblSuppressed"></span></div>
</div></div></section></main></body></html>"""


def test_parcel_span_form():
    body = ('<span id="ctlBodyPane_ctl05_ctl01_sprLnkOwnerName1_lnkUpmSearchLinkSuppressed_lblSearch">'
            'ABBOTT REBECCA LYNN, SMITH BRENDA JEAN</span>\n'
            '<span id="ctlBodyPane_ctl05_ctl01_sprLblOwnerName2_lblSuppressed">'
            '<br>OR THARP BRANDON LEE</span>')
    owner, mail = parse_qpublic_owner(
        TPL.format(BODY=body, A1="2024 SLATE RD", A2="",
                   CSZ="ELLENWOOD GA 30294 2213"))
    assert owner == "ABBOTT REBECCA LYNN, SMITH BRENDA JEAN, OR THARP BRANDON LEE", owner
    assert mail == ["2024 SLATE RD", "ELLENWOOD GA 30294 2213"], mail


def test_parcel_anchor_form():
    body = ('<a id="ctlBodyPane_ctl05_ctl01_sprLnkOwnerName1_lnkUpmSearchLinkSuppressed_lnkSearch" '
            'href="javascript:__doPostBack(\'x\',\'\')">JOHNSON BOBBY Q</a>\n'
            '<span id="ctlBodyPane_ctl05_ctl01_sprLblOwnerName2_lblSuppressed"></span>')
    owner, mail = parse_qpublic_owner(
        TPL.format(BODY=body, A1="302 HILLCREST CIRCLE", A2="",
                   CSZ="ANDERSON SC 29624 "))
    assert owner == "JOHNSON BOBBY Q", owner
    assert mail == ["302 HILLCREST CIRCLE", "ANDERSON SC 29624"], mail


def test_no_owner_section():
    assert parse_qpublic_owner("<html><body><p>nothing here</p></body></html>") == ("", [])


def test_blank_owner_block():
    owner, mail = parse_qpublic_owner(
        TPL.format(BODY="", A1="", A2="", CSZ=""))
    assert (owner, mail) == ("", [])


# Coweta County's Beacon template: the Owner section is a table, not
# four-column-blocks. Verbatim structure captured 2026-10-02 from the live
# Coweta qPublic report for parcel W09 030.
COWETA_TPL = """<html><body><main id="maincontent">
<section id="ctlBodyPane_ctl03_mSection">
<header class="module-header">
<div id="ctlBodyPane_ctl03_lblName" class="title">Owner</div>
</header>
<div class="module-content">
<table class="tabular-data-two-column" style="width:100%" role="presentation">
<tbody><tr>
<th scope="row">
<span id="ctlBodyPane_ctl03_ctl01_lnkOwnerName_lblSearch">  SAMPLES KENNETH L &amp; CHRISTY N SAMPLES </span>
<span id="ctlBodyPane_ctl03_ctl01_lblAddress"><br>266 YORKSHIRE PLACE</span>
<span id="ctlBodyPane_ctl03_ctl01_lblCityStateZip"><br>NEWNAN, GA 30265</span>
</th>
<td style="width:70%;">
<span id="ctlBodyPane_ctl03_ctl01_lblMultiowner"></span>
</td>
</tr></tbody></table>
</div></section></main></body></html>"""


def test_coweta_table_template():
    owner, mail = parse_qpublic_owner(COWETA_TPL)
    assert owner == "SAMPLES KENNETH L & CHRISTY N SAMPLES", owner
    assert mail == ["266 YORKSHIRE PLACE", "NEWNAN, GA 30265"], mail


# Rockdale County's Beacon template: the same table shape as Coweta's --
# Rockdale's Owner section renders name/street/city-state-zip in one cell
# (verified 2026-10-03 from the crawled report for parcel 045B010022; the
# fixture below reproduces that structure with the parcel's real owner data).
ROCKDALE_TPL = """<html><body><main id="maincontent">
<section id="ctlBodyPane_ctl03_mSection">
<header class="module-header">
<div id="ctlBodyPane_ctl03_lblName" class="title">Owner</div>
</header>
<div class="module-content">
<table class="tabular-data-two-column" style="width:100%" role="presentation">
<tbody><tr>
<th scope="row">
<span id="ctlBodyPane_ctl03_ctl01_lnkOwnerName_lblSearch">LEAPHART PAMELA JOY</span>
<span id="ctlBodyPane_ctl03_ctl01_lblAddress"><br>1584 CHERRY HIL CT SW</span>
<span id="ctlBodyPane_ctl03_ctl01_lblCityStateZip"><br>CONYERS, GA 30094</span>
</th>
<td style="width:70%;">
<span id="ctlBodyPane_ctl03_ctl01_lblMultiowner"></span>
</td>
</tr></tbody></table>
</div></section></main></body></html>"""


def test_rockdale_table_template():
    owner, mail = parse_qpublic_owner(ROCKDALE_TPL)
    assert owner == "LEAPHART PAMELA JOY", owner
    assert mail == ["1584 CHERRY HIL CT SW", "CONYERS, GA 30094"], mail


if __name__ == "__main__":
    test_parcel_span_form()
    test_parcel_anchor_form()
    test_no_owner_section()
    test_blank_owner_block()
    test_coweta_table_template()
    test_rockdale_table_template()
    print("ALL QPARSER TESTS PASSED")

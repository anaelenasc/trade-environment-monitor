#!/usr/bin/env python3
"""
fetch_data.py  —  Trade Policy and Green Transition Monitor
Fetches intervention records from the GTA v1 data API and aggregates
into indicators for the dashboard.

API docs: https://github.com/global-trade-alert/docs/blob/main/.api/gta-data.md
Endpoint: POST https://api.globaltradealert.org/api/v1/data/
Auth:     Authorization: APIKey {key}
"""
import os, json, csv, sys, time
from datetime import date
from collections import defaultdict
import requests

# ── CONFIG ─────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("GTA_API_KEY", "")
if not API_KEY:
    sys.exit("ERROR: GTA_API_KEY environment variable not set.")

BASE_URL  = "https://api.globaltradealert.org/api/v1/data/"
HEADERS   = {"Authorization": f"APIKey {API_KEY}", "Content-Type": "application/json"}
DATE_FROM = "2020-01-01"
PAGE_SIZE = 1000


# ── HS CODE DEFINITIONS ────────────────────────────────────────────────────
def _dedup(lst):
    return list(dict.fromkeys(int(x) for x in lst))

_BATTERIES   = [850610,850630,850640,850650,850660,850680,850690,
                850710,850720,850730,850750,850760,850780,850790]
_FUEL_CELL   = [840590,850690]
_OTHER_GREEN = [
    870380,854142,854143,850231,854330,252100,252220,281610,701990,
    841410,841430,841440,841480,841490,841780,841960,841989,842139,
    842199,842490,851411,851419,851420,851431,851432,851439,851490,
    280110,281410,281511,281512,281830,282010,282090,282410,283210,
    283220,283510,283524,283525,283526,283529,380210,392690,580190,
    730900,731010,731021,731029,732510,841011,841012,841013,841090,
    841320,841350,841360,841370,841381,842119,842121,842129,842191,
    842381,842382,842389,848110,848130,848140,848180,902610,902620,
]
_SOLAR = [
    392690,700991,700992,711590,730431,730441,730451,730890,741121,
    741122,741129,761090,830630,841280,841989,841990,847989,850110,
    850120,850131,850171,850172,850132,850133,850134,850140,850151,
    850152,850153,850161,850162,850163,850164,850180,850239,850300,
    850440,854142,854143,900190,900290,900580,901380,
]
_WIND = [
    730820,730890,841290,848210,848220,848230,848240,848250,848280,
    848340,850161,850162,850163,850164,850231,850421,850422,850423,
    850431,850432,850433,850434,853710,853720,854442,854449,854460,
    902830,903020,903031,903032,903033,903039,903289,
]

GREEN_GOODS_HS  = _dedup(_BATTERIES + _FUEL_CELL + _OTHER_GREEN + _SOLAR + _WIND)
GREEN_GOODS_SET = set(GREEN_GOODS_HS)

HYDROGEN_HS    = [280410]
BIOFUEL_HS     = [382600]
GREEN_FUELS_HS = _dedup(HYDROGEN_HS + BIOFUEL_HS)

MINERAL_HS = {
    "lithium":    [282520, 283691],
    "copper":     [260300,262030,282550,282741,283325,740100,740200,740311,740312,
                   740313,740319,740321,740322,740329,740400,740500,740610,740620,
                   740710,740721,740729,740811,740819,740821,740822,740829,740911,
                   740919,740921,740929,740931,740939,740940,740990,
                   741011,741012,741013,741014],
    "cobalt":     [260500, 282200, 810520, 810530, 810590],
    "nickel":     [260400,282540,282735,283324,720260,750110,750120,750210,750220,
                   750300,750400,750511,750512,750521,750522,750610,750620],
    "graphite":   [250410, 250490],
    "rare_earths":[280530, 284610, 284690],
}
ALL_MINERALS_HS = _dedup([c for codes in MINERAL_HS.values() for c in codes])

COAL_HS  = [270111,270112,270119,270120,270210,270220,270300,270400]
OIL_HS   = [270900,271012,271019,271020,271112,271113,271114,271119,
            271121,271129,271210,271220,271290,271311,271312,271320,271390]
GAS_HS   = [271111,271112,271113,271114,271119,271121,271129]
PETRO_HS = [290121,290122,290123,290124,290211,290220,290230,290241,290242,
            290243,290244,290250,290511,290512,290513,290514,290516,290531,
            290532,291521,291611,291612,291736,292910,281410,281420,310210,
            310230,310240,390110,390120,390210,390230,390311,390319,390330,
            390410,390690,390760,400219,400220,400259]
FOSSIL_HS = _dedup(COAL_HS + OIL_HS + GAS_HS + PETRO_HS)

ALL_GREEN_HS = _dedup(GREEN_GOODS_HS + GREEN_FUELS_HS + ALL_MINERALS_HS)


# ── FILTER HELPERS ─────────────────────────────────────────────────────────
IMPORT_BARRIER_TYPES = {
    "Import tariff","Import quota","Import ban","Import tariff quota",
    "Import licensing requirement","Import price benchmark",
    "Minimum import price","Other import charges",
}
EXPORT_CONTROL_TYPES = {
    "Export ban","Export quota","Export tax","Export tariff quota",
    "Export licensing requirement","Export price benchmark",
    "Local supply requirement for exports",
}
TRADE_REMEDY_TYPES = {
    "Anti-dumping","Anti-subsidy","Safeguard","Anti-circumvention",
}

def is_subsidy(r):
    return "ubsidi" in r.get("mast_chapter", "")

def is_harmful(r):
    return r.get("gta_evaluation", "") in ("Red", "Amber")

def is_liberalising(r):
    return r.get("gta_evaluation", "") == "Green"


def get_product_ids(r):
    """Return set of HS codes from an intervention record (handles both access levels)."""
    prods = r.get("affected_products", [])
    if not prods:
        return set()
    if isinstance(prods[0], dict):          # full access: list of objects
        return {p["product_id"] for p in prods}
    return set(prods)                       # basic access: list of integers


# ── API FETCH ──────────────────────────────────────────────────────────────
def _fetch_page(hs_codes: list, offset: int, label: str) -> list | None:
    """
    Single paginated request. Returns the page list, [] for no results,
    or None on a hard API error (after 3 retries).
    """
    req  = {"affected_products": hs_codes, "announcement_period": [DATE_FROM, None]}
    body = {"limit": PAGE_SIZE, "offset": offset, "request_data": req}
    for attempt in range(3):
        try:
            resp = requests.post(BASE_URL, headers=HEADERS, json=body, timeout=180)
            resp.raise_for_status()
            page = resp.json()
            return page if isinstance(page, list) else []
        except requests.RequestException as exc:
            status = getattr(getattr(exc, "response", None), "status_code", "–")
            if attempt == 2:
                print(f"\n  ✗ API error (offset={offset}, HTTP {status}): {exc}",
                      file=sys.stderr)
                return None
            time.sleep(5 * (attempt + 1))
    return None


def fetch_all(hs_codes: list, label: str = "") -> list:
    """
    Paginated fetch for hs_codes. Automatically batches codes in groups of
    BATCH_SIZE so a single invalid code cannot silently zero out the whole
    fetch. Results are deduplicated by intervention id across batches.
    """
    BATCH_SIZE = 8
    batches = [hs_codes[i:i+BATCH_SIZE] for i in range(0, len(hs_codes), BATCH_SIZE)]
    all_records: dict[int, dict] = {}   # id → record, for dedup

    for b_idx, batch in enumerate(batches):
        offset = 0
        batch_label = f"{label}[b{b_idx+1}/{len(batches)}]" if len(batches) > 1 else label
        while True:
            page = _fetch_page(batch, offset, batch_label)
            if page is None:            # hard error — skip this batch
                break
            if not page:                # empty page — done with this batch
                break
            for r in page:
                rid = r.get("id") or r.get("intervention_id")
                if rid:
                    all_records[rid] = r
                else:
                    all_records[id(r)] = r
            print(f"    {batch_label} {len(all_records)} records…", end="\r")
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

    records = list(all_records.values())
    print(f"    {label}: {len(records)} unique records fetched")
    return records


# ── AGGREGATION ────────────────────────────────────────────────────────────

def compute_net_policy_stance(green_records: list) -> list:
    """Liberalising minus harmful on GREEN GOODS only, per (implementer, year)."""
    agg = defaultdict(lambda: {"harmful": 0, "liberalising": 0, "country": ""})

    for r in green_records:
        # Restrict to green goods only (exclude hydrogen / minerals)
        if not (get_product_ids(r) & GREEN_GOODS_SET):
            continue
        year  = int(r["date_announced"][:4])
        eval_ = r.get("gta_evaluation", "")
        for jur in r.get("implementing_jurisdictions", []):
            iso = jur.get("iso", "")
            if not iso:
                continue
            agg[(iso, year)]["country"] = jur.get("name", "")
            if eval_ in ("Red", "Amber"):
                agg[(iso, year)]["harmful"]      += 1
            elif eval_ == "Green":
                agg[(iso, year)]["liberalising"] += 1

    return sorted(
        [{"iso": iso, "country": d["country"], "year": year,
          "harmful": d["harmful"], "liberalising": d["liberalising"]}
         for (iso, year), d in agg.items()],
        key=lambda x: (x["iso"], x["year"]),
    )


def compute_green_sector_support(green_records: list) -> list:
    """
    Harmful subsidies (MAST L) + liberalising import barriers + liberalising
    export controls on green goods / hydrogen / minerals — per implementer.
    """
    sup = defaultdict(lambda: {"country": "", "count": 0})

    for r in green_records:
        itype = r.get("intervention_type", "")
        qualifies = (
            (is_subsidy(r) and is_harmful(r)) or
            (itype in IMPORT_BARRIER_TYPES and is_liberalising(r)) or
            (itype in EXPORT_CONTROL_TYPES  and is_liberalising(r))
        )
        if not qualifies:
            continue
        for jur in r.get("implementing_jurisdictions", []):
            iso = jur.get("iso", "")
            if iso:
                sup[iso]["country"] = jur.get("name", "")
                sup[iso]["count"]  += 1

    out = [{"iso": iso, "country": d["country"], "count": d["count"]}
           for iso, d in sup.items()]
    out.sort(key=lambda x: -x["count"])
    return out[:20]


def compute_green_industrial_growth(green_records: list) -> list:
    """Annual counts of all interventions on green/hydrogen/minerals."""
    agg = defaultdict(lambda: {"harmful": 0, "liberalising": 0})
    for r in green_records:
        year  = int(r["date_announced"][:4])
        eval_ = r.get("gta_evaluation", "")
        if eval_ in ("Red", "Amber"):
            agg[year]["harmful"]      += 1
        elif eval_ == "Green":
            agg[year]["liberalising"] += 1
    return [{"year": y, "harmful": d["harmful"], "liberalising": d["liberalising"]}
            for y, d in sorted(agg.items())]


def compute_fossil_fuel_support(fossil_records: list) -> list:
    """Currently in-force subsidy measures on fossil fuels, per implementer."""
    sup = defaultdict(lambda: {"country": "", "count": 0})
    for r in fossil_records:
        if not (is_subsidy(r) and r.get("is_in_force") == 1):
            continue
        for jur in r.get("implementing_jurisdictions", []):
            iso = jur.get("iso", "")
            if iso:
                sup[iso]["country"] = jur.get("name", "")
                sup[iso]["count"]  += 1
    out = [{"iso": iso, "country": d["country"], "count": d["count"]}
           for iso, d in sup.items()]
    out.sort(key=lambda x: -x["count"])
    return out


def compute_fossil_vs_green_trend(fossil_records: list, green_records: list) -> list:
    """Annual MAST-L counts: fossil fuels vs green/hydrogen/minerals."""
    fossil_by_year = defaultdict(int)
    green_by_year  = defaultdict(int)
    for r in fossil_records:
        if is_subsidy(r):
            fossil_by_year[int(r["date_announced"][:4])] += 1
    for r in green_records:
        if is_subsidy(r):
            green_by_year[int(r["date_announced"][:4])] += 1
    years = sorted(set(fossil_by_year) | set(green_by_year))
    return [{"year": y, "fossil": fossil_by_year.get(y, 0), "green": green_by_year.get(y, 0)}
            for y in years]


def compute_trade_remedies(green_records: list) -> list:
    """Trade remedies (AD/AS/SG/AC) on green goods, per implementer."""
    sup = defaultdict(lambda: {"country": "", "count": 0})
    for r in green_records:
        if r.get("intervention_type", "") not in TRADE_REMEDY_TYPES:
            continue
        if not (get_product_ids(r) & GREEN_GOODS_SET):
            continue
        for jur in r.get("implementing_jurisdictions", []):
            iso = jur.get("iso", "")
            if iso:
                sup[iso]["country"] = jur.get("name", "")
                sup[iso]["count"]  += 1
    out = [{"iso": iso, "country": d["country"], "count": d["count"]}
           for iso, d in sup.items()]
    out.sort(key=lambda x: -x["count"])
    return out[:20]


def _itype_matches(r, type_set):
    """
    Check intervention_type against a set, handling both string (v2 API)
    and list (v1 API inconsistency) return values.
    """
    itype = r.get("intervention_type", "")
    if isinstance(itype, list):
        return any(t in type_set for t in itype)
    return itype in type_set


def compute_mineral_export_restrictions(mineral_records: dict, all_mineral_records: list) -> dict:
    """
    Export controls per mineral per year, plus an 'all' aggregate.

    Per-mineral: each mineral's dedicated fetch, deduplicated by intervention id.
    All-minerals: computed from all_mineral_records (already fetched as part of
    green_records), filtered to mineral HS codes and deduplicated — avoids
    double-counting interventions that cover multiple minerals.

    Diagnostic output is printed per mineral to help identify fetch vs filter issues.
    """

    def _series(records, label=""):
        by_year = defaultdict(set)
        seen_itypes = set()
        skipped = 0
        for r in records:
            itype = r.get("intervention_type", "")
            seen_itypes.add(str(itype)[:60])
            if not _itype_matches(r, EXPORT_CONTROL_TYPES):
                skipped += 1
                continue
            announced = r.get("date_announced")
            if not announced:
                continue
            year = int(str(announced)[:4])
            rid  = r.get("id") or r.get("intervention_id") or id(r)
            by_year[year].add(rid)
        matched = sum(len(v) for v in by_year.values())
        print(f"    {label}: {len(records)} records fetched, "
              f"{matched} export controls matched, {skipped} skipped")
        if len(records) > 0 and matched == 0:
            print(f"      ↳ intervention types seen: {sorted(seen_itypes)[:10]}")
        return [{"year": y, "count": len(ids)} for y, ids in sorted(by_year.items())]

    out = {}
    print("  Mineral export restriction counts:")
    for mineral, records in mineral_records.items():
        out[mineral] = _series(records, label=mineral)

    # All-minerals aggregate — deduplicated across minerals
    mineral_set = set(ALL_MINERALS_HS)
    mineral_records_all = [r for r in all_mineral_records
                           if get_product_ids(r) & mineral_set]
    out["all"] = _series(mineral_records_all, label="all minerals")

    return out


# ── CSV EXPORT ─────────────────────────────────────────────────────────────
def write_csv(indicators: dict):
    rows = []
    for r in indicators.get("net_policy_stance", []):
        rows.append({"indicator":"net_policy_stance","country":r["country"],
                     "iso":r["iso"],"year":r["year"],
                     "value":r["liberalising"]-r["harmful"],"sub_value":""})
    for r in indicators.get("green_sector_support", []):
        rows.append({"indicator":"green_sector_support","country":r["country"],
                     "iso":r["iso"],"year":"cumulative_since_2020",
                     "value":r["count"],"sub_value":""})
    for r in indicators.get("fossil_fuel_support", []):
        rows.append({"indicator":"fossil_fuel_support_active","country":r["country"],
                     "iso":r["iso"],"year":"in_force",
                     "value":r["count"],"sub_value":""})
    for r in indicators.get("trade_remedies", []):
        rows.append({"indicator":"trade_remedies_on_green_goods","country":r["country"],
                     "iso":r["iso"],"year":"cumulative_since_2020",
                     "value":r["count"],"sub_value":""})
    for r in indicators.get("green_industrial_growth", []):
        rows.append({"indicator":"green_industrial_growth_harmful","country":"Global",
                     "iso":"WLD","year":r["year"],"value":r["harmful"],"sub_value":""})
        rows.append({"indicator":"green_industrial_growth_liberalising","country":"Global",
                     "iso":"WLD","year":r["year"],"value":r["liberalising"],"sub_value":""})
    for r in indicators.get("fossil_vs_green_trend", []):
        rows.append({"indicator":"fossil_subsidy_trend","country":"Global",
                     "iso":"WLD","year":r["year"],"value":r["fossil"],"sub_value":""})
        rows.append({"indicator":"green_subsidy_trend","country":"Global",
                     "iso":"WLD","year":r["year"],"value":r["green"],"sub_value":""})
    for mineral, series in indicators.get("mineral_export_restrictions", {}).items():
        for r in series:
            rows.append({"indicator":"mineral_export_restrictions","country":"Global",
                         "iso":"WLD","year":r["year"],"value":r["count"],
                         "sub_value":mineral})

    with open("data/indicators_export.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["indicator","country","iso","year","value","sub_value"])
        w.writeheader()
        w.writerows(rows)
    print("  ✓ data/indicators_export.csv written")


# ── STATIC FTA DATA ─────────────────────────────────────────────────────────
# ── STATIC FTA DATA (source: CLAIRK database) ─────────────────────────────
# Total agreements tracked: 64
FTA_TOTAL_PAIRS = 64

FTA_COUNTRY_PAIRS = [
    # EU (19 agreements — shown via bloc/bilateral nodes)
    {"from":"EU","to":"Canada",         "agreement":"CETA",                          "year":2017,"provisions":1},
    {"from":"EU","to":"Israel",         "agreement":"EU-Israel Association",         "year":2000,"provisions":1},
    {"from":"EU","to":"C. America",     "agreement":"Central America-EU Association","year":2013,"provisions":2},
    {"from":"EU","to":"ESA",            "agreement":"ESA-EU Interim EPA",            "year":2012,"provisions":1},
    {"from":"EU","to":"Kenya",          "agreement":"EU-Kenya EPA",                  "year":2023,"provisions":1},
    {"from":"EU","to":"W. Balkans",     "agreement":"EU Stabilisation & Association Agreements","year":2006,"provisions":5,
     "countries":"Albania, Bosnia-Herzegovina, Kosovo, North Macedonia, Serbia"},
    {"from":"EU","to":"Mediterr.",      "agreement":"EU Association Agreements",     "year":2002,"provisions":9,
     "countries":"Algeria, Azerbaijan, Egypt, Iraq, Jordan, Lebanon, Morocco, Palestinian Authority, Tunisia"},
    # Korea
    {"from":"Korea","to":"US",          "agreement":"Korea-US FTA",            "year":2012,"provisions":1},
    {"from":"Korea","to":"Australia",   "agreement":"Australia-Korea FTA",     "year":2014,"provisions":1},
    {"from":"Korea","to":"Israel",      "agreement":"Israel-Korea FTA",        "year":2021,"provisions":1},
    {"from":"Korea","to":"NZ",          "agreement":"Korea-NZ FTA",            "year":2015,"provisions":2},
    {"from":"Korea","to":"Peru",        "agreement":"Korea-Peru FTA",          "year":2011,"provisions":4},
    {"from":"Korea","to":"Colombia",    "agreement":"Colombia-Korea FTA",      "year":2016,"provisions":1},
    {"from":"Korea","to":"C. America",  "agreement":"Central America-Korea FTA","year":2019,"provisions":1},
    # Canada
    {"from":"Canada","to":"US",         "agreement":"USMCA",                   "year":2020,"provisions":2},
    {"from":"Canada","to":"Israel",     "agreement":"Canada-Israel FTA",       "year":1997,"provisions":1},
    {"from":"Canada","to":"Chile",      "agreement":"CPTPP",                   "year":2018,"provisions":2},
    {"from":"Canada","to":"NZ",         "agreement":"CPTPP",                   "year":2018,"provisions":2},
    {"from":"Canada","to":"Peru",       "agreement":"CPTPP + Canada-Peru FTA", "year":2018,"provisions":4},
    {"from":"Canada","to":"Colombia",   "agreement":"Canada-Colombia FTA",     "year":2011,"provisions":2},
    {"from":"Canada","to":"Japan",      "agreement":"CPTPP",                   "year":2018,"provisions":2},
    {"from":"Canada","to":"Mediterr.",  "agreement":"Canada-Jordan FTA",       "year":2012,"provisions":1,
     "countries":"Jordan"},
    # UK
    {"from":"UK","to":"Australia",      "agreement":"Australia-UK FTA",        "year":2023,"provisions":3},
    {"from":"UK","to":"NZ",             "agreement":"NZ-UK FTA",               "year":2023,"provisions":2},
    {"from":"UK","to":"ESA",            "agreement":"ESA-UK EPA",              "year":2021,"provisions":1},
    {"from":"UK","to":"Kenya",          "agreement":"Kenya-UK EPA",            "year":2021,"provisions":1},
    {"from":"UK","to":"India",          "agreement":"India-UK FTA",            "year":2024,"provisions":2},
    # US
    {"from":"US","to":"Australia",      "agreement":"Australia-US FTA",        "year":2005,"provisions":1},
    {"from":"US","to":"Chile",          "agreement":"Chile-US FTA",            "year":2004,"provisions":3},
    {"from":"US","to":"Peru",           "agreement":"Peru-US TPA",             "year":2009,"provisions":1},
    {"from":"US","to":"Colombia",       "agreement":"Colombia-US TPA",         "year":2012,"provisions":1},
    {"from":"US","to":"C. America",     "agreement":"CAFTA-DR",                "year":2006,"provisions":1},
    {"from":"US","to":"Oman",           "agreement":"Oman-US FTA",             "year":2009,"provisions":1},
    {"from":"US","to":"Singapore",      "agreement":"Singapore-US FTA",        "year":2004,"provisions":1},
    {"from":"US","to":"Panama",         "agreement":"Panama-US TPA",           "year":2012,"provisions":1},
    {"from":"US","to":"Mediterr.",      "agreement":"Morocco-US FTA",          "year":2006,"provisions":1,
     "countries":"Morocco"},
    # CPTPP bilateral pairs
    {"from":"Australia","to":"Chile",   "agreement":"CPTPP","year":2018,"provisions":2},
    {"from":"Australia","to":"NZ",      "agreement":"CPTPP","year":2018,"provisions":2},
    {"from":"Australia","to":"Peru",    "agreement":"CPTPP","year":2018,"provisions":2},
    {"from":"Australia","to":"Japan",   "agreement":"CPTPP","year":2018,"provisions":2},
    {"from":"Chile","to":"NZ",          "agreement":"CPTPP","year":2018,"provisions":2},
    {"from":"Chile","to":"Peru",        "agreement":"CPTPP","year":2018,"provisions":2},
    {"from":"Chile","to":"Japan",       "agreement":"CPTPP","year":2018,"provisions":2},
    {"from":"NZ","to":"Peru",           "agreement":"CPTPP","year":2018,"provisions":2},
    {"from":"NZ","to":"Japan",          "agreement":"CPTPP","year":2018,"provisions":2},
    {"from":"Peru","to":"Japan",        "agreement":"CPTPP","year":2018,"provisions":2},
    # Israel
    {"from":"Israel","to":"Colombia",   "agreement":"Colombia-Israel FTA","year":2020,"provisions":1},
    # UAE
    {"from":"UAE","to":"Australia",     "agreement":"Australia-UAE CEPA","year":2023,"provisions":2},
    {"from":"UAE","to":"NZ",            "agreement":"NZ-UAE CEPA",        "year":2023,"provisions":1},
    {"from":"UAE","to":"Chile",         "agreement":"Chile-UAE CEPA",     "year":2024,"provisions":1},
]

EU_MEMBERS = ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA",
               "DEU","GRC","HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD",
               "POL","PRT","ROU","SVK","SVN","ESP","SWE"]

FTA_COVERAGE = (
    [{"iso": iso, "count": 19} for iso in EU_MEMBERS] +
    [{"iso":"USA","count":11},{"iso":"KOR","count":8},{"iso":"CAN","count":8},
     {"iso":"NZL","count":6},{"iso":"ISR","count":6},{"iso":"GBR","count":5},
     {"iso":"AUS","count":5},{"iso":"PER","count":4},{"iso":"PAN","count":4},
     {"iso":"MYS","count":4},{"iso":"GTM","count":4},{"iso":"COL","count":4},
     {"iso":"CHL","count":4},{"iso":"ARE","count":3},{"iso":"CHE","count":3},
     {"iso":"SGP","count":3},{"iso":"NIC","count":3},{"iso":"MEX","count":3},
     {"iso":"JPN","count":3},{"iso":"HND","count":3},{"iso":"SLV","count":3},
     {"iso":"CRI","count":3},{"iso":"ZWE","count":2},{"iso":"ZMB","count":2},
     {"iso":"UKR","count":2},{"iso":"SYC","count":2},{"iso":"MAR","count":2},
     {"iso":"MUS","count":2},{"iso":"MDG","count":2},{"iso":"KEN","count":2},
     {"iso":"JOR","count":2},{"iso":"ISL","count":2},{"iso":"COM","count":2},
     {"iso":"TWN","count":2},{"iso":"CHN","count":2},{"iso":"VNM","count":1},
     {"iso":"TUR","count":1},{"iso":"TUN","count":1},{"iso":"SRB","count":1},
     {"iso":"OMN","count":1},{"iso":"NOR","count":1},{"iso":"MKD","count":1},
     {"iso":"LBN","count":1},{"iso":"IRQ","count":1},{"iso":"IND","count":1},
     {"iso":"EGY","count":1},{"iso":"DOM","count":1},{"iso":"BRN","count":1},
     {"iso":"BIH","count":1},{"iso":"AZE","count":1},{"iso":"DZA","count":1},
     {"iso":"ALB","count":1}]
)


# ── MAIN ────────────────────────────────────────────────────────────────────
def main():
    print("=== Trade Policy and Green Transition Monitor — data pipeline ===")
    print(f"Run date: {date.today().isoformat()}\n")

    # Fetch 1: all green goods / fuels / minerals
    print("Fetch 1/3  All green interventions (goods + fuels + minerals)…")
    green_records = fetch_all(ALL_GREEN_HS, label="green")
    print(f"  → {len(green_records)} records total\n")

    # Fetch 2: all fossil fuel interventions
    print("Fetch 2/3  All fossil fuel interventions…")
    fossil_records = fetch_all(FOSSIL_HS, label="fossil")
    print(f"  → {len(fossil_records)} records total\n")

    # Fetch 3: per-mineral (for export restrictions trend)
    print("Fetch 3/3  Per-mineral export restriction data…")
    mineral_records = {}
    for mineral, hs_codes in MINERAL_HS.items():
        print(f"  → {mineral}…")
        mineral_records[mineral] = fetch_all(hs_codes, label=mineral)

    print("\nAggregating indicators…")
    indicators = {
        "last_updated": date.today().isoformat(),
        "data_notes": (
            "Dynamic indicators derived from the Global Trade Alert database "
            "(globaltradealert.org). All counts reflect announced interventions "
            "from 1 January 2020. HS 281410 appears in both green goods (Other) "
            "and petrochemicals; interventions on this code count toward both. "
            "FTA data is static and updated manually."
        ),
        "fta_country_pairs":            FTA_COUNTRY_PAIRS,
        "fta_total_pairs":              FTA_TOTAL_PAIRS,
        "fta_coverage":                 FTA_COVERAGE,
        "net_policy_stance":            compute_net_policy_stance(green_records),
        "green_sector_support":         compute_green_sector_support(green_records),
        "green_industrial_growth":      compute_green_industrial_growth(green_records),
        "fossil_fuel_support":          compute_fossil_fuel_support(fossil_records),
        "fossil_vs_green_trend":        compute_fossil_vs_green_trend(fossil_records, green_records),
        "trade_remedies":               compute_trade_remedies(green_records),
        "mineral_export_restrictions":  compute_mineral_export_restrictions(mineral_records, green_records),
    }

    os.makedirs("data", exist_ok=True)
    with open("data/indicators.json", "w") as f:
        json.dump(indicators, f, indent=2)
    print(f"✓ data/indicators.json written ({date.today().isoformat()})")
    write_csv(indicators)
    print("\n=== Pipeline complete ===")


if __name__ == "__main__":
    main()


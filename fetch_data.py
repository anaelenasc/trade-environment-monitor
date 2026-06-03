#!/usr/bin/env python3
"""
fetch_data.py  —  Trade Policy and Green Transition Monitor
Daily incremental update pipeline.

Strategy:
  1. Load data/records_cache.json (built once by seed_cache.py)
  2. Fetch only records published since last_fetched via date_published filter
  3. Upsert into cache by intervention_id
  4. Recompute all indicators from the full cache
  5. Write data/indicators.json and data/interventions_download.csv

First-run fallback: if no cache exists, fetches everything since DATE_FROM
(same as the original approach) and builds the cache from scratch.
"""
import os, json, csv, sys, time
from datetime import date
from collections import defaultdict
import requests

# ── CONFIG ─────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("GTA_API_KEY", "")
if not API_KEY:
    sys.exit("ERROR: GTA_API_KEY environment variable not set.")

BASE_URL  = "https://api-staging.globaltradealert.org/api/v1/data/"
HEADERS   = {"Authorization": f"APIKey {API_KEY}", "Content-Type": "application/json"}
DATE_FROM = "2020-01-01"
PAGE_SIZE = 100
CACHE_PATH = "data/records_cache.json"


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

HYDROGEN_HS     = [280410]
BIOFUEL_HS      = [382600]
GREEN_FUELS_HS  = _dedup(HYDROGEN_HS + BIOFUEL_HS)

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
ALL_MINERALS_HS  = _dedup([c for codes in MINERAL_HS.values() for c in codes])
ALL_MINERALS_SET = set(ALL_MINERALS_HS)
MINERAL_SETS     = {k: set(v) for k, v in MINERAL_HS.items()}

COAL_HS  = [270111,270112,270119,270120,270210,270220,270300,270400]
OIL_HS   = [270900,271012,271019,271020,271112,271113,271114,271119,
            271121,271129,271210,271220,271290,271311,271312,271320,271390]
GAS_HS   = [271111,271112,271113,271114,271119,271121,271129]
PETRO_HS = [290121,290122,290123,290124,290211,290220,290230,290241,290242,
            290243,290244,290250,290511,290512,290513,290514,290516,290531,
            290532,291521,291611,291612,291736,292910,281410,281420,310210,
            310230,310240,390110,390120,390210,390230,390311,390319,290330,
            390410,390690,390760,400219,400220,400259]
FOSSIL_HS  = _dedup(COAL_HS + OIL_HS + GAS_HS + PETRO_HS)
FOSSIL_SET = set(FOSSIL_HS)

ALL_GREEN_HS  = _dedup(GREEN_GOODS_HS + GREEN_FUELS_HS + ALL_MINERALS_HS)
ALL_GREEN_SET = set(ALL_GREEN_HS)
RELEVANT_SET  = ALL_GREEN_SET | FOSSIL_SET


# ── INTERVENTION FILTER SETS ───────────────────────────────────────────────
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

# ── NAME → ISO (used when normalising API records missing iso field) ────────
NAME_ISO = {
    "Afghanistan":"AFG","Albania":"ALB","Algeria":"DZA","Angola":"AGO",
    "Anguilla":"AIA","Antigua & Barbuda":"ATG","Argentina":"ARG","Armenia":"ARM",
    "Australia":"AUS","Austria":"AUT","Azerbaijan":"AZE","Bahamas":"BHS",
    "Bahrain":"BHR","Bangladesh":"BGD","Belarus":"BLR","Belgium":"BEL",
    "Belize":"BLZ","Benin":"BEN","Bermuda":"BMU","Bhutan":"BTN","Bolivia":"BOL",
    "Bosnia & Herzegovina":"BIH","Botswana":"BWA","Brazil":"BRA",
    "Brunei Darussalam":"BRN","Bulgaria":"BGR","Burkina Faso":"BFA",
    "Burundi":"BDI","Cambodia":"KHM","Cameroon":"CMR","Canada":"CAN",
    "Cape Verde":"CPV","Central African Republic":"CAF","Chad":"TCD",
    "Chile":"CHL","China":"CHN","Chinese Taipei":"TWN","Colombia":"COL",
    "Comoros":"COM","Congo":"COG","Costa Rica":"CRI","Croatia":"HRV",
    "Cuba":"CUB","Cyprus":"CYP","Czechia":"CZE","DR Congo":"COD",
    "Denmark":"DNK","Djibouti":"DJI","Dominican Republic":"DOM","Ecuador":"ECU",
    "Egypt":"EGY","El Salvador":"SLV","Equatorial Guinea":"GNQ","Eritrea":"ERI",
    "Estonia":"EST","Eswatini":"SWZ","Ethiopia":"ETH","Faeroe Islands":"FRO",
    "Fiji":"FJI","Finland":"FIN","France":"FRA","Gabon":"GAB","Gambia":"GMB",
    "Georgia":"GEO","Germany":"DEU","Ghana":"GHA","Greece":"GRC",
    "Guatemala":"GTM","Guinea":"GIN","Guinea-Bissau":"GNB","Guyana":"GUY",
    "Haiti":"HTI","Honduras":"HND","Hong Kong":"HKG","Hungary":"HUN",
    "Iceland":"ISL","India":"IND","Indonesia":"IDN","Iran":"IRN","Iraq":"IRQ",
    "Ireland":"IRL","Israel":"ISR","Italy":"ITA","Ivory Coast":"CIV",
    "Jamaica":"JAM","Japan":"JPN","Jordan":"JOR","Kazakhstan":"KAZ",
    "Kenya":"KEN","Kuwait":"KWT","Kyrgyzstan":"KGZ","Lao":"LAO","Latvia":"LVA",
    "Lebanon":"LBN","Lesotho":"LSO","Liberia":"LBR","Libya":"LBY",
    "Liechtenstein":"LIE","Lithuania":"LTU","Luxembourg":"LUX",
    "Macedonia":"MKD","Madagascar":"MDG","Malawi":"MWI","Malaysia":"MYS",
    "Maldives":"MDV","Mali":"MLI","Malta":"MLT","Mauritania":"MRT",
    "Mauritius":"MUS","Mexico":"MEX","Mongolia":"MNG","Montenegro":"MNE",
    "Montserrat":"MSR","Morocco":"MAR","Mozambique":"MOZ","Myanmar":"MMR",
    "Namibia":"NAM","Nepal":"NPL","Netherlands":"NLD","New Caledonia":"NCL",
    "New Zealand":"NZL","Nicaragua":"NIC","Niger":"NER","Nigeria":"NGA",
    "Norway":"NOR","Oman":"OMN","Pakistan":"PAK","Panama":"PAN",
    "Paraguay":"PRY","Peru":"PER","Philippines":"PHL","Poland":"POL",
    "Portugal":"PRT","Qatar":"QAT","Republic of Korea":"KOR",
    "Republic of Moldova":"MDA","Republic of the Sudan":"SDN","Romania":"ROU",
    "Russia":"RUS","Rwanda":"RWA","Saint Kitts & Nevis":"KNA",
    "Saint Lucia":"LCA","Saint Vincent & the Grenadines":"VCT","Samoa":"WSM",
    "Sao Tome & Principe":"STP","Saudi Arabia":"SAU","Senegal":"SEN",
    "Serbia":"SRB","Seychelles":"SYC","Sierra Leone":"SLE","Singapore":"SGP",
    "Slovakia":"SVK","Slovenia":"SVN","Solomon Islands":"SLB","Somalia":"SOM",
    "South Africa":"ZAF","South Sudan":"SSD","Spain":"ESP","Sri Lanka":"LKA",
    "State of Palestine":"PSE","Suriname":"SUR","Sweden":"SWE",
    "Switzerland":"CHE","Syria":"SYR","Tajikistan":"TJK","Tanzania":"TZA",
    "Thailand":"THA","Togo":"TGO","Trinidad & Tobago":"TTO","Tunisia":"TUN",
    "Turkiye":"TUR","Turkmenistan":"TKM","Turks & Caicos Islands":"TCA",
    "Uganda":"UGA","Ukraine":"UKR","United Arab Emirates":"ARE",
    "United Kingdom":"GBR","United States of America":"USA","Uruguay":"URY",
    "Uzbekistan":"UZB","Venezuela":"VEN","Vietnam":"VNM",
    "Western Sahara":"ESH","Yemen":"YEM","Zambia":"ZMB","Zimbabwe":"ZWE",
}


# ── RECORD HELPERS ─────────────────────────────────────────────────────────
def get_product_ids(r: dict) -> set:
    prods = r.get("affected_products", [])
    if not prods:
        return set()
    if isinstance(prods[0], dict):
        return {p["product_id"] for p in prods}
    return set(prods)

def get_year(r: dict) -> int:
    return int(r["date_announced"][:4])

def is_harmful(r: dict) -> bool:
    return (r.get("gta_evaluation") or "") in ("Red", "Amber")

def is_liberalising(r: dict) -> bool:
    return (r.get("gta_evaluation") or "") == "Green"

def is_subsidy(r: dict) -> bool:
    return "ubsidi" in (r.get("mast_chapter") or "")

def is_in_force(r: dict) -> bool:
    return r.get("is_in_force") == 1


# ── NORMALISE API RECORD ───────────────────────────────────────────────────
def normalise_api_record(r: dict) -> dict:
    """
    Normalise an API response record to the cache format.
    Ensures implementing_jurisdictions always has iso codes,
    and affected_products is always a list of integers.
    """
    # Normalise jurisdictions: add iso if missing
    jurs = []
    for j in r.get("implementing_jurisdictions", []):
        name = j.get("name", "")
        iso  = j.get("iso", "") or NAME_ISO.get(name, "")
        jurs.append({"name": name, "iso": iso})

    # Normalise products to list of ints
    raw_prods = r.get("affected_products", [])
    if raw_prods and isinstance(raw_prods[0], dict):
        prods = [p["product_id"] for p in raw_prods]
    else:
        prods = [int(p) for p in raw_prods if p is not None]

    return {
        "intervention_id":            r.get("intervention_id"),
        "intervention_url":           r.get("intervention_url", ""),
        "gta_evaluation":             r.get("gta_evaluation") or "",
        "implementing_jurisdictions": jurs,
        "intervention_type":          r.get("intervention_type") or "",
        "mast_chapter":               r.get("mast_chapter") or "",
        "affected_products":          prods,
        "date_announced":             r.get("date_announced", ""),
        "is_in_force":                r.get("is_in_force", 0),
    }


# ── CACHE ──────────────────────────────────────────────────────────────────
def load_cache() -> tuple[dict, str | None]:
    """Returns (records_dict, last_fetched_date_or_None)."""
    if not os.path.exists(CACHE_PATH):
        print(f"  No cache found at {CACHE_PATH} — will do full fetch.")
        return {}, None
    with open(CACHE_PATH) as f:
        cache = json.load(f)
    records      = cache.get("records", {})
    last_fetched = cache.get("last_fetched", None)
    print(f"  Cache loaded: {len(records)} records, last fetched {last_fetched}")
    return records, last_fetched


def save_cache(records: dict):
    os.makedirs("data", exist_ok=True)
    cache = {"last_fetched": date.today().isoformat(), "records": records}
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, separators=(",", ":"))
    size_mb = os.path.getsize(CACHE_PATH) / 1e6
    print(f"  ✓ Cache saved: {len(records)} records ({size_mb:.1f} MB)")


# ── API FETCH ──────────────────────────────────────────────────────────────
def _fetch_page(body: dict) -> list:
    for attempt in range(3):
        try:
            resp = requests.post(BASE_URL, headers=HEADERS, json=body, timeout=180)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == 2:
                print(f"\n  ✗ API error: {exc}", file=sys.stderr)
                return []
            time.sleep(10 * (attempt + 1))
    return []


def fetch_records(since_date: str | None) -> list:
    """
    Fetch interventions announced since DATE_FROM.
    If since_date is provided, also filter by date_published >= since_date
    so only new/updated records are returned.
    """
    req = {"announcement_period": [DATE_FROM, None]}
    if since_date:
        req["date_published"] = [since_date, None]
        print(f"  Incremental fetch: published on or after {since_date}…")
    else:
        print(f"  Full fetch: all interventions since {DATE_FROM}…")

    records = []
    offset  = 0
    while True:
        body = {"limit": PAGE_SIZE, "offset": offset, "request_data": req}
        page = _fetch_page(body)
        if not page:
            break
        records.extend(page)
        print(f"  {len(records)} records fetched…", end="\r")
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(1.0)

    print(f"\n  → {len(records)} records fetched")
    return records


# ── AGGREGATION ────────────────────────────────────────────────────────────
def compute_net_policy_stance(records: list) -> list:
    agg = defaultdict(lambda: {"harmful": 0, "liberalising": 0, "country": ""})
    for r in records:
        if not (get_product_ids(r) & GREEN_GOODS_SET):
            continue
        year  = get_year(r)
        eval_ = r.get("gta_evaluation") or ""
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


def compute_green_sector_support(records: list) -> list:
    sup = defaultdict(lambda: {"country": "", "count": 0})
    for r in records:
        if not (get_product_ids(r) & ALL_GREEN_SET):
            continue
        itype = r.get("intervention_type") or ""
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


def compute_green_industrial_growth(records: list) -> list:
    agg = defaultdict(lambda: {"harmful": 0, "liberalising": 0})
    for r in records:
        if not (get_product_ids(r) & ALL_GREEN_SET):
            continue
        year  = get_year(r)
        eval_ = r.get("gta_evaluation") or ""
        if eval_ in ("Red", "Amber"):
            agg[year]["harmful"]      += 1
        elif eval_ == "Green":
            agg[year]["liberalising"] += 1
    return [{"year": y, "harmful": d["harmful"], "liberalising": d["liberalising"]}
            for y, d in sorted(agg.items())]


def compute_fossil_fuel_support(records: list) -> list:
    sup = defaultdict(lambda: {"country": "", "count": 0})
    for r in records:
        if not (get_product_ids(r) & FOSSIL_SET):
            continue
        if not (is_subsidy(r) and is_in_force(r)):
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


def compute_fossil_vs_green_trend(records: list) -> list:
    fossil_by_year = defaultdict(int)
    green_by_year  = defaultdict(int)
    for r in records:
        if not is_subsidy(r):
            continue
        prods = get_product_ids(r)
        year  = get_year(r)
        if prods & FOSSIL_SET:
            fossil_by_year[year] += 1
        if prods & ALL_GREEN_SET:
            green_by_year[year]  += 1
    years = sorted(set(fossil_by_year) | set(green_by_year))
    return [{"year": y,
             "fossil": fossil_by_year.get(y, 0),
             "green":  green_by_year.get(y, 0)}
            for y in years]


def compute_trade_remedies(records: list) -> list:
    sup = defaultdict(lambda: {"country": "", "count": 0})
    for r in records:
        if (r.get("intervention_type") or "") not in TRADE_REMEDY_TYPES:
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


def compute_mineral_export_restrictions(records: list) -> dict:
    out = {}
    for mineral, mineral_set in MINERAL_SETS.items():
        by_year = defaultdict(int)
        for r in records:
            if (r.get("intervention_type") or "") not in EXPORT_CONTROL_TYPES:
                continue
            if get_product_ids(r) & mineral_set:
                by_year[get_year(r)] += 1
        out[mineral] = [{"year": y, "count": c} for y, c in sorted(by_year.items())]
    return out


# ── DOWNLOAD CSV ───────────────────────────────────────────────────────────
def write_interventions_csv(records_dict: dict):
    """
    Write data/interventions_download.csv — one row per relevant intervention.
    Jurisdictions and HS codes are comma-separated within their cells.
    """
    fieldnames = [
        "intervention_id", "intervention_url", "date_announced",
        "gta_evaluation", "intervention_type", "mast_chapter",
        "is_in_force", "implementing_jurisdictions", "affected_hs_codes",
    ]
    rows = []
    for r in records_dict.values():
        if not (get_product_ids(r) & RELEVANT_SET):
            continue
        rows.append({
            "intervention_id":          r.get("intervention_id", ""),
            "intervention_url":         r.get("intervention_url", ""),
            "date_announced":           r.get("date_announced", ""),
            "gta_evaluation":           r.get("gta_evaluation", ""),
            "intervention_type":        r.get("intervention_type", ""),
            "mast_chapter":             r.get("mast_chapter", ""),
            "is_in_force":              "Yes" if r.get("is_in_force") == 1 else "No",
            "implementing_jurisdictions": ", ".join(
                j.get("name", "") for j in r.get("implementing_jurisdictions", [])
                if j.get("name")
            ),
            "affected_hs_codes": ", ".join(
                str(p) for p in sorted(get_product_ids(r))
            ),
        })

    rows.sort(key=lambda x: x["date_announced"], reverse=True)

    with open("data/interventions_download.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ data/interventions_download.csv written ({len(rows)} interventions)")


# ── INDICATORS CSV ─────────────────────────────────────────────────────────
def write_indicators_csv(indicators: dict):
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
                     "iso":r["iso"],"year":"in_force","value":r["count"],"sub_value":""})
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
    with open("data/indicators_export.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["indicator","country","iso","year","value","sub_value"])
        w.writeheader()
        w.writerows(rows)
    print("  ✓ data/indicators_export.csv written")


# ── STATIC FTA DATA ─────────────────────────────────────────────────────────
FTA_COUNTRY_PAIRS = [
    {"from":"EU",    "to":"Japan",       "agreement":"EU-Japan EPA",        "year":2019,"provisions":25},
    {"from":"EU",    "to":"Canada",      "agreement":"CETA",                "year":2017,"provisions":31},
    {"from":"EU",    "to":"South Korea", "agreement":"EU-South Korea FTA",  "year":2011,"provisions":18},
    {"from":"EU",    "to":"Singapore",   "agreement":"EU-Singapore FTA",    "year":2019,"provisions":12},
    {"from":"EU",    "to":"UK",          "agreement":"EU-UK TCA",           "year":2021,"provisions":40},
    {"from":"EU",    "to":"New Zealand", "agreement":"EU-New Zealand FTA",  "year":2024,"provisions":15},
    {"from":"US",    "to":"Canada",      "agreement":"USMCA",               "year":2020,"provisions":20},
    {"from":"US",    "to":"Mexico",      "agreement":"USMCA",               "year":2020,"provisions":18},
    {"from":"Japan", "to":"Australia",   "agreement":"JAEPA",               "year":2015,"provisions":10},
    {"from":"Japan", "to":"UK",          "agreement":"Japan-UK EPA",        "year":2021,"provisions":22},
    {"from":"UK",    "to":"Australia",   "agreement":"UK-Australia FTA",    "year":2023,"provisions":24},
    {"from":"UK",    "to":"New Zealand", "agreement":"UK-NZ FTA",           "year":2023,"provisions":19},
    {"from":"Canada","to":"Australia",   "agreement":"CPTPP",               "year":2018,"provisions":8},
    {"from":"Japan", "to":"Mexico",      "agreement":"Japan-Mexico EPA",    "year":2005,"provisions":7},
]


# ── MAIN ────────────────────────────────────────────────────────────────────
def main():
    print("=== Trade Policy and Green Transition Monitor — data pipeline ===")
    print(f"Run date: {date.today().isoformat()}\n")

    # 1. Load cache
    print("Loading cache…")
    records_dict, last_fetched = load_cache()

    # 2. Fetch new/updated records from API
    print("\nFetching from API…")
    new_records = fetch_records(since_date=last_fetched)

    # 3. Upsert into cache
    updated = 0
    added   = 0
    for r in new_records:
        normalised = normalise_api_record(r)
        rid = str(normalised["intervention_id"])
        if rid in records_dict:
            updated += 1
        else:
            added += 1
        records_dict[rid] = normalised

    print(f"  Upserted: {added} new, {updated} updated. Cache total: {len(records_dict)}")

    # 4. Save updated cache
    print("\nSaving cache…")
    save_cache(records_dict)

    # 5. Compute indicators from full cache
    print("\nAggregating indicators…")
    records_list = list(records_dict.values())

    indicators = {
        "last_updated": date.today().isoformat(),
        "data_notes": (
            "Dynamic indicators derived from the Global Trade Alert database "
            "(globaltradealert.org). All counts reflect announced interventions "
            "from 1 January 2020. HS 281410 appears in both green goods (Other) "
            "and petrochemicals; interventions on this code count toward both. "
            "FTA data is static and updated manually."
        ),
        "net_policy_stance":            compute_net_policy_stance(records_list),
        "green_sector_support":         compute_green_sector_support(records_list),
        "green_industrial_growth":      compute_green_industrial_growth(records_list),
        "fossil_fuel_support":          compute_fossil_fuel_support(records_list),
        "fossil_vs_green_trend":        compute_fossil_vs_green_trend(records_list),
        "trade_remedies":               compute_trade_remedies(records_list),
        "mineral_export_restrictions":  compute_mineral_export_restrictions(records_list),
    }

    os.makedirs("data", exist_ok=True)
    with open("data/indicators.json", "w") as f:
        json.dump(indicators, f, indent=2)
    print(f"  ✓ data/indicators.json written")

    # 6. Write download files
    print("\nWriting download files…")
    write_interventions_csv(records_dict)
    write_indicators_csv(indicators)

    print("\n=== Pipeline complete ===")


if __name__ == "__main__":
    main()
  

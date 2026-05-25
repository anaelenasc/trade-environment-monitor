#!/usr/bin/env python3
"""
fetch_data.py
Trade Policy and Green Transition Monitor — GTA data pipeline.

Fetches intervention counts from the GTA API and writes data/indicators.json.
Run daily via GitHub Actions.

NOTE: HS 281410 appears in both OTHER_GREEN_HS (green goods) and PETRO_HS
(petrochemicals). Interventions on this code will appear in both green and
fossil fuel indicators. All other category boundaries are clean.
"""

import os, json, sys, time, csv
from datetime import date
from collections import defaultdict
import requests

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("GTA_API_KEY", "")
if not API_KEY:
    sys.exit("ERROR: GTA_API_KEY environment variable not set.")

BASE_URL = "https://api.globaltradealert.org/api/v2/gta/count/"
HEADERS  = {"Authorization": f"Token {API_KEY}", "Content-Type": "application/json"}
DATE_FROM = "2020-01-01"


# ── HS CODE DEFINITIONS ───────────────────────────────────────────────────────
def _dedup(lst):
    """Deduplicate while preserving order; coerce to int."""
    return list(dict.fromkeys(int(x) for x in lst))

# --- Green goods ---
_BATTERIES = [
    850610, 850630, 850640, 850650, 850660, 850680, 850690,
    850710, 850720, 850730, 850750, 850760, 850780, 850790,
]
_FUEL_CELL = [840590, 850690]

_OTHER_GREEN = [
    # 280410 removed per product table revision
    870380, 854142, 854143, 850231, 854330, 252100, 252220,
    281610, 701990, 841410, 841430, 841440, 841480, 841490, 841780,
    841960, 841989, 842139, 842199, 842490, 851411, 851419, 851420,
    851431, 851432, 851439, 851490, 252100, 252220, 280110, 281410,
    281511, 281512, 281610, 281830, 282010, 282090, 282410, 283210,
    283220, 283510, 283524, 283525, 283526, 283529, 380210, 392690,
    580190, 730900, 731010, 731021, 731029, 732510, 841011, 841012,
    841013, 841090, 841320, 841350, 841360, 841370, 841381, 841430,
    841440, 841480, 841490, 841780, 842119, 842121, 842129, 842191,
    842199, 842381, 842382, 842389, 842490, 848110, 848130, 848140,
    848180, 851411, 851419, 851420, 851431, 851432, 851439, 851490,
    902610, 902620,
]
_SOLAR = [
    392690, 700991, 700992, 711590, 730431, 730441, 730451, 730890,
    741121, 741122, 741129, 761090, 830630, 841280, 841989, 841990,
    847989, 850110, 850120, 850131, 850171, 850172, 850132, 850133,
    850134, 850140, 850151, 850152, 850153, 850161, 850162, 850163,
    850164, 850180, 850239, 850300, 850440, 854142, 854143, 900190,
    900290, 900580, 901380,
]
_WIND = [
    730820, 730890, 841290, 848210, 848220, 848230, 848240, 848250,
    848280, 848340, 850161, 850162, 850163, 850164, 850231, 850421,
    850422, 850423, 850431, 850432, 850433, 850434, 853710, 853720,
    854442, 854449, 854460, 902830, 903020, 903031, 903032, 903033,
    903039, 903289,
]

GREEN_GOODS_HS = _dedup(_BATTERIES + _FUEL_CELL + _OTHER_GREEN + _SOLAR + _WIND)

# --- Green fuels ---
HYDROGEN_HS    = [280410]
BIOFUEL_HS     = [382600]
GREEN_FUELS_HS = _dedup(HYDROGEN_HS + BIOFUEL_HS)

# --- Strategic minerals ---
MINERAL_HS = {
    "lithium":    [282520, 283691],
    "copper":     [
        260300, 262030, 282550, 282741, 283325, 740100, 740200,
        740311, 740312, 740313, 740319, 740321, 740322, 740329,
        740400, 740500, 740610, 740620, 740710, 740721, 740729,
        740811, 740819, 740821, 740822, 740829, 740911, 740919,
        740921, 740929, 740931, 740939, 740940, 740990,
        741011, 741012, 741013, 741014,
    ],
    "cobalt":     [260500, 282200, 810520, 810530, 810590],
    "nickel":     [
        260400, 282540, 282735, 283324, 720260, 750110, 750120,
        750210, 750220, 750300, 750400, 750511, 750512,
        750521, 750522, 750610, 750620,
    ],
    "graphite":   [250410, 250490],
    "rare_earths":[280530, 284610, 284690],
}
ALL_MINERALS_HS = _dedup([c for codes in MINERAL_HS.values() for c in codes])

# --- Fossil fuels (clean — no shared codes with green categories) ---
COAL_HS  = [270111, 270112, 270119, 270120, 270210, 270220, 270300, 270400]
OIL_HS   = [
    270900, 271012, 271019, 271020, 271112, 271113, 271114, 271119,
    271121, 271129, 271210, 271220, 271290, 271311, 271312, 271320, 271390,
]
GAS_HS   = [271111, 271112, 271113, 271114, 271119, 271121, 271129]  # revised
PETRO_HS = [
    # 290121-290124 no longer in gas — overlap resolved
    290121, 290122, 290123, 290124, 290211, 290220, 290230,
    290241, 290242, 290243, 290244, 290250, 290511, 290512,
    290513, 290514, 290516, 290531, 290532, 291521, 291611,
    291612, 291736, 292910, 281410, 281420, 310210, 310230,
    310240, 390110, 390120, 390210, 390230, 390311, 390319,
    390330, 390410, 390690, 390760, 400219, 400220, 400259,
]
FOSSIL_HS = _dedup(COAL_HS + OIL_HS + GAS_HS + PETRO_HS)

# --- Combined sets ---
ALL_GREEN_HS = _dedup(GREEN_GOODS_HS + GREEN_FUELS_HS + ALL_MINERALS_HS)


# ── INTERVENTION TYPE DEFINITIONS ─────────────────────────────────────────────
IMPORT_BARRIER_TYPES = [
    "Import tariff", "Import quota", "Import ban", "Import tariff quota",
    "Import licensing requirement", "Import price benchmark",
    "Minimum import price", "Other import charges",
]
EXPORT_CONTROL_TYPES = [
    "Export ban", "Export quota", "Export tax", "Export tariff quota",
    "Export licensing requirement", "Export price benchmark",
    "Local supply requirement for exports",
]
TRADE_REMEDY_TYPES = [
    "Anti-dumping", "Anti-subsidy", "Safeguard", "Anti-circumvention",
]


# ── API HELPER ────────────────────────────────────────────────────────────────
def gta_count(body: dict) -> list:
    """POST to GTA count endpoint with exponential-backoff retry."""
    for attempt in range(3):
        try:
            r = requests.post(BASE_URL, headers=HEADERS, json=body, timeout=180)
            r.raise_for_status()
            return r.json().get("results", [])
        except requests.RequestException as exc:
            if attempt == 2:
                print(f"  ✗ API call failed after 3 attempts: {exc}", file=sys.stderr)
                return []
            time.sleep(5 * (attempt + 1))
    return []


def get_year(record: dict):
    """Extract announcement year from a count result row (handles API key variants)."""
    for key in ("date_announced_year_value", "date_announced_year", "year"):
        val = record.get(key)
        if val is not None:
            try:
                return int(val)
            except (ValueError, TypeError):
                pass
    return None


# ── FETCH FUNCTIONS ───────────────────────────────────────────────────────────

def fetch_net_policy_stance() -> list:
    """
    Harmful vs liberalising interventions on green goods, by country and year.
    Net = liberalising − (red + amber) per jurisdiction per year.
    """
    print("  Fetching net policy stance on green goods…")
    rows = gta_count({
        "affected_products": GREEN_GOODS_HS,
        "date_announced_gte": DATE_FROM,
        "count_by": ["implementer", "gta_evaluation", "date_announced_year"],
    })
    agg = defaultdict(lambda: {"harmful": 0, "liberalising": 0, "country": ""})
    for r in rows:
        iso  = r.get("implementer_iso", "")
        name = r.get("implementer_name", "")
        year = get_year(r)
        eval_ = r.get("gta_evaluation_name", "")
        val   = r.get("value", 0)
        if not iso or not year:
            continue
        key = (iso, year)
        agg[key]["country"] = name
        if eval_ in ("Red", "Amber"):
            agg[key]["harmful"] += val
        elif eval_ == "Green":
            agg[key]["liberalising"] += val

    return sorted(
        [{"iso": iso, "country": d["country"], "year": year,
          "harmful": d["harmful"], "liberalising": d["liberalising"]}
         for (iso, year), d in agg.items()],
        key=lambda x: (x["iso"], x["year"]),
    )


def fetch_green_sector_support() -> list:
    """
    Combined green sector support measures per country (cumulative since 2020):
      (a) harmful subsidies (MAST L) on green goods / hydrogen / minerals
      (b) liberalising import barriers on green goods / hydrogen / minerals
      (c) liberalising export controls on green goods / hydrogen / minerals
    Returns top 20 sorted descending.
    """
    print("  Fetching green sector support measures…")
    support = defaultdict(lambda: {"country": "", "count": 0})

    def _add(rows):
        for r in rows:
            iso = r.get("implementer_iso", "")
            if iso:
                support[iso]["country"] = r.get("implementer_name", "")
                support[iso]["count"]  += r.get("value", 0)

    # (a) harmful subsidies
    _add(gta_count({
        "affected_products": ALL_GREEN_HS,
        "mast_chapters": ["L"],
        "gta_evaluation": ["Red", "Amber"],
        "date_announced_gte": DATE_FROM,
        "count_by": ["implementer"],
    }))
    # (b) liberalising import barriers
    _add(gta_count({
        "affected_products": ALL_GREEN_HS,
        "intervention_types": IMPORT_BARRIER_TYPES,
        "gta_evaluation": ["Green"],
        "date_announced_gte": DATE_FROM,
        "count_by": ["implementer"],
    }))
    # (c) liberalising export controls
    _add(gta_count({
        "affected_products": ALL_GREEN_HS,
        "intervention_types": EXPORT_CONTROL_TYPES,
        "gta_evaluation": ["Green"],
        "date_announced_gte": DATE_FROM,
        "count_by": ["implementer"],
    }))

    out = [{"iso": iso, "country": d["country"], "count": d["count"]}
           for iso, d in support.items()]
    out.sort(key=lambda x: -x["count"])
    return out[:20]


def fetch_green_industrial_growth() -> list:
    """Annual counts of all interventions on green goods / hydrogen / minerals since 2020."""
    print("  Fetching green industrial policy growth trend…")
    rows = gta_count({
        "affected_products": ALL_GREEN_HS,
        "date_announced_gte": DATE_FROM,
        "count_by": ["date_announced_year", "gta_evaluation"],
    })
    agg = defaultdict(lambda: {"harmful": 0, "liberalising": 0})
    for r in rows:
        year  = get_year(r)
        eval_ = r.get("gta_evaluation_name", "")
        val   = r.get("value", 0)
        if not year:
            continue
        if eval_ in ("Red", "Amber"):
            agg[year]["harmful"] += val
        elif eval_ == "Green":
            agg[year]["liberalising"] += val

    return [{"year": y, "harmful": d["harmful"], "liberalising": d["liberalising"]}
            for y, d in sorted(agg.items())]


def fetch_fossil_fuel_support() -> list:
    """
    Active fossil fuel subsidy measures (MAST L, in force) by implementing jurisdiction.
    """
    print("  Fetching active fossil fuel support measures…")
    rows = gta_count({
        "affected_products": FOSSIL_HS,
        "mast_chapters": ["L"],
        "status": "in_force",
        "date_announced_gte": DATE_FROM,
        "count_by": ["implementer"],
    })
    out = [{"iso": r.get("implementer_iso", ""),
            "country": r.get("implementer_name", ""),
            "count": r.get("value", 0)}
           for r in rows if r.get("implementer_iso")]
    out.sort(key=lambda x: -x["count"])
    return out


def fetch_fossil_vs_green_trend() -> list:
    """
    Annual counts of MAST L interventions:
      fossil fuels vs green goods / hydrogen / minerals.
    """
    print("  Fetching fossil vs green subsidy trend…")
    fossil_rows = gta_count({
        "affected_products": FOSSIL_HS,
        "mast_chapters": ["L"],
        "date_announced_gte": DATE_FROM,
        "count_by": ["date_announced_year"],
    })
    green_rows = gta_count({
        "affected_products": ALL_GREEN_HS,
        "mast_chapters": ["L"],
        "date_announced_gte": DATE_FROM,
        "count_by": ["date_announced_year"],
    })

    def _to_dict(rows):
        d = {}
        for r in rows:
            y = get_year(r)
            if y:
                d[y] = d.get(y, 0) + r.get("value", 0)
        return d

    fossil_by_year = _to_dict(fossil_rows)
    green_by_year  = _to_dict(green_rows)
    years = sorted(set(fossil_by_year) | set(green_by_year))
    return [{"year": y,
             "fossil": fossil_by_year.get(y, 0),
             "green":  green_by_year.get(y, 0)}
            for y in years]


def fetch_trade_remedies() -> list:
    """
    Cumulative counts of anti-dumping / anti-subsidy / safeguard / anti-circumvention
    interventions on green goods by implementing jurisdiction since 2020.
    Returns top 20.
    """
    print("  Fetching trade remedies on green goods…")
    rows = gta_count({
        "affected_products": GREEN_GOODS_HS,
        "intervention_types": TRADE_REMEDY_TYPES,
        "date_announced_gte": DATE_FROM,
        "count_by": ["implementer"],
    })
    out = [{"iso": r.get("implementer_iso", ""),
            "country": r.get("implementer_name", ""),
            "count": r.get("value", 0)}
           for r in rows if r.get("implementer_iso")]
    out.sort(key=lambda x: -x["count"])
    return out[:20]


def fetch_mineral_export_restrictions() -> dict:
    """
    Annual counts of export controls per strategic mineral since 2020.
    Returns dict keyed by mineral name.
    """
    print("  Fetching export restrictions on strategic minerals…")
    out = {}
    for mineral, hs_codes in MINERAL_HS.items():
        print(f"    → {mineral}…")
        rows = gta_count({
            "affected_products": hs_codes,
            "intervention_types": EXPORT_CONTROL_TYPES,
            "date_announced_gte": DATE_FROM,
            "count_by": ["date_announced_year"],
        })
        by_year = {}
        for r in rows:
            y = get_year(r)
            if y:
                by_year[y] = by_year.get(y, 0) + r.get("value", 0)
        out[mineral] = [{"year": y, "count": c} for y, c in sorted(by_year.items())]
    return out


# ── EXPORT CSV ────────────────────────────────────────────────────────────────
def write_indicators_csv(indicators: dict):
    """Write a flat CSV of key indicator values for download."""
    rows = []

    # Net policy stance
    for r in indicators.get("net_policy_stance", []):
        rows.append({
            "indicator": "net_policy_stance",
            "country": r["country"], "iso": r["iso"],
            "year": r["year"],
            "value": r["liberalising"] - r["harmful"],
            "sub_value": "",
        })

    # Green sector support
    for r in indicators.get("green_sector_support", []):
        rows.append({
            "indicator": "green_sector_support",
            "country": r["country"], "iso": r["iso"],
            "year": "cumulative_since_2020",
            "value": r["count"], "sub_value": "",
        })

    # Fossil fuel support
    for r in indicators.get("fossil_fuel_support", []):
        rows.append({
            "indicator": "fossil_fuel_support_active",
            "country": r["country"], "iso": r["iso"],
            "year": "in_force",
            "value": r["count"], "sub_value": "",
        })

    # Trade remedies
    for r in indicators.get("trade_remedies", []):
        rows.append({
            "indicator": "trade_remedies_on_green_goods",
            "country": r["country"], "iso": r["iso"],
            "year": "cumulative_since_2020",
            "value": r["count"], "sub_value": "",
        })

    # Trends
    for r in indicators.get("green_industrial_growth", []):
        rows.append({"indicator": "green_industrial_growth_harmful",
                     "country": "Global", "iso": "WLD",
                     "year": r["year"], "value": r["harmful"], "sub_value": ""})
        rows.append({"indicator": "green_industrial_growth_liberalising",
                     "country": "Global", "iso": "WLD",
                     "year": r["year"], "value": r["liberalising"], "sub_value": ""})

    for r in indicators.get("fossil_vs_green_trend", []):
        rows.append({"indicator": "fossil_subsidy_trend",
                     "country": "Global", "iso": "WLD",
                     "year": r["year"], "value": r["fossil"], "sub_value": ""})
        rows.append({"indicator": "green_subsidy_trend",
                     "country": "Global", "iso": "WLD",
                     "year": r["year"], "value": r["green"], "sub_value": ""})

    # Mineral export restrictions
    for mineral, series in indicators.get("mineral_export_restrictions", {}).items():
        for r in series:
            rows.append({"indicator": "mineral_export_restrictions",
                         "country": "Global", "iso": "WLD",
                         "year": r["year"], "value": r["count"],
                         "sub_value": mineral})

    with open("data/indicators_export.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["indicator","country","iso","year","value","sub_value"])
        writer.writeheader()
        writer.writerows(rows)
    print("  ✓ data/indicators_export.csv written")


# ── STATIC FTA MOCK DATA ──────────────────────────────────────────────────────
# Replace with real data once the FTA xlsx is available.
FTA_COUNTRY_PAIRS = [
    {"from": "EU",  "to": "Japan",       "agreement": "EU-Japan EPA",      "year": 2019, "provisions": 25},
    {"from": "EU",  "to": "Canada",      "agreement": "CETA",              "year": 2017, "provisions": 31},
    {"from": "EU",  "to": "South Korea", "agreement": "EU-South Korea FTA","year": 2011, "provisions": 18},
    {"from": "EU",  "to": "Singapore",   "agreement": "EU-Singapore FTA",  "year": 2019, "provisions": 12},
    {"from": "EU",  "to": "UK",          "agreement": "EU-UK TCA",         "year": 2021, "provisions": 40},
    {"from": "EU",  "to": "New Zealand", "agreement": "EU-New Zealand FTA","year": 2024, "provisions": 15},
    {"from": "US",  "to": "Canada",      "agreement": "USMCA",             "year": 2020, "provisions": 20},
    {"from": "US",  "to": "Mexico",      "agreement": "USMCA",             "year": 2020, "provisions": 18},
    {"from": "Japan","to":"Australia",   "agreement": "JAEPA",             "year": 2015, "provisions": 10},
    {"from": "Japan","to":"UK",          "agreement": "Japan-UK EPA",      "year": 2021, "provisions": 22},
    {"from": "UK",  "to": "Australia",   "agreement": "UK-Australia FTA",  "year": 2023, "provisions": 24},
    {"from": "UK",  "to": "New Zealand", "agreement": "UK-NZ FTA",         "year": 2023, "provisions": 19},
    {"from": "Canada","to":"Australia",  "agreement": "CPTPP",             "year": 2018, "provisions": 8},
    {"from": "Japan","to":"Mexico",      "agreement": "Japan-Mexico EPA",  "year": 2005, "provisions": 7},
]


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Trade Policy and Green Transition Monitor — data pipeline ===")
    print(f"Run date: {date.today().isoformat()}\n")

    indicators = {
        "last_updated": date.today().isoformat(),
        "data_notes": (
            "Dynamic indicators are derived from the Global Trade Alert database "
            "(globaltradealert.org). All counts reflect announced interventions from "
            "1 January 2020. HS 281410 appears in both green goods (Other green goods) "
            "and fossil fuels (Petrochemicals); interventions on this code count toward "
            "both categories. FTA data is static and updated manually."
        ),
        # Static (mock until real xlsx is provided)
        "fta_country_pairs": FTA_COUNTRY_PAIRS,
        "fta_total_pairs": len(FTA_COUNTRY_PAIRS),
        # Dynamic
        "net_policy_stance":            fetch_net_policy_stance(),
        "green_sector_support":         fetch_green_sector_support(),
        "green_industrial_growth":      fetch_green_industrial_growth(),
        "fossil_fuel_support":          fetch_fossil_fuel_support(),
        "fossil_vs_green_trend":        fetch_fossil_vs_green_trend(),
        "trade_remedies":               fetch_trade_remedies(),
        "mineral_export_restrictions":  fetch_mineral_export_restrictions(),
    }

    os.makedirs("data", exist_ok=True)

    with open("data/indicators.json", "w") as f:
        json.dump(indicators, f, indent=2)
    print(f"\n✓ data/indicators.json written ({date.today().isoformat()})")

    write_indicators_csv(indicators)
    print("\n=== Pipeline complete ===")


if __name__ == "__main__":
    main()

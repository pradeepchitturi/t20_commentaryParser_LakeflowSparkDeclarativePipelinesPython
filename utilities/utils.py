# utilities/utils.py
#
# Shared configuration, UDFs, and helper functions for the Cricket Commentary
# DLT pipeline. This file is imported by all transformation notebooks.
#
# Usage in DLT pipeline source files:
#   %run ../utilities/utils
#
# Or reference this file alongside transformations in the Pipeline's
# "Source code" paths — Databricks will execute all listed paths.

from pyspark.sql.functions import (
    col, when, regexp_extract, regexp_replace, trim, first, create_map,
    coalesce, collect_set, lit, current_timestamp, split, size,
    sum as spark_sum, udf, to_timestamp, concat, to_utc_timestamp,
    to_date, row_number, lead, count, countDistinct,
    min as _min, max as _max, floor, round as _round, lower
)
from pyspark.sql.types import (
    StringType, LongType, IntegerType, BooleanType, DoubleType
)
from pyspark.sql.window import Window
import json
import time

# ── Configure these in the DLT Pipeline UI ──────────────────────────────────
# Settings -> Configuration -> Add key/value pairs
#
#  Key                            Default
#  ─────────────────────────────────────────────────────────────────────────
#  pipeline.source_path           abfss://cricinfo-mens-international@...
#  pipeline.catalog               T20_catalog
#  pipeline.schema_location_base  abfss://cricinfo-mens-international@.../dlt_schemas
#  pipeline.run_id                dlt_prod  (optional — used as Batchid)

# NOTE: The catalog is set once in the DLT Pipeline UI under Settings → Target → Catalog.
# Here we only define the schema (layer) names used in @dlt.table(schema=...) and dlt.read().
# DLT's schema= parameter accepts the schema name only — not "catalog.schema".
  # kept for reference / logging
# Schema inference state path — must be ABFSS or Unity Catalog Volume
# (not dbfs:/ on serverless compute)
SOURCE_PATH  = "abfss://cricinfo-mens-international@adlschitturidemo.dfs.core.windows.net/"
SCHEMA_BASE  = "abfss://cricinfo-mens-international@adlschitturidemo.dfs.core.windows.net/dlt_schemas"
BRONZE = "bronze"
SILVER = "silver"
GOLD   = "gold"

KNOWN_TEAM_ABBREVS = {
    "USA": "United States Of America",
    "NED": "Netherlands",
    "BHR": "Bahrain",
    "NZ":  "New Zealand",
    "WI":  "West Indies",
    "SL":  "Sri Lanka",
    "PNG": "Papua New Guinea",
    "UAE": "United Arab Emirates",
    "HK":  "Hong Kong",
    "RSA": "South Africa",
    "SA":  "South Africa",
    "ENG": "England",
    "AUS": "Australia",
    "IND": "India",
    "PAK": "Pakistan",
    "BAN": "Bangladesh",
    "AFG": "Afghanistan",
    "ZIM": "Zimbabwe",
    "IRE": "Ireland",
    "SCO": "Scotland",
    "NAM": "Namibia",
    "NEP": "Nepal",
    "OMA": "Oman",
    "CAN": "Canada",
    "UGA": "Uganda",
}

@udf(StringType())
def get_full_name(short_name, name_list):
    """
    Resolve an abbreviated player or team name to its full form.

    Strategy (in order):
      1. Direct match — short_name already in the name_list
      2. Known team abbreviation lookup (KNOWN_TEAM_ABBREVS dict)
      3. Initials match — all initials of words in the full name
      4. Upper-case initials only match
      5. Prefix match (short_name is a prefix of the full name)
      6. Last-name match
      7. Last N words match

    Returns short_name unchanged if no match is found.

    REQUIRES: installed as a cluster library — not pip installable in DLT.
    """
    if not short_name or not name_list:
        return short_name

    names = name_list if isinstance(name_list, list) else (
        json.loads(name_list) if isinstance(name_list, str) else []
    )

    if not names or short_name in names or "Super Over" in short_name:
        return short_name

    abbr       = short_name.strip()
    abbr_upper = abbr.upper()
    abbr_parts = abbr.split()

    # 1. Known team abbreviation
    if abbr_upper in KNOWN_TEAM_ABBREVS:
        known_full = KNOWN_TEAM_ABBREVS[abbr_upper]
        for name in names:
            if name.upper() == known_full.upper():
                return name

    for name in names:
        name_parts = name.split()
        name_upper = name.upper()

        # 2. All-initials match  (e.g. "RG" -> "Rohit Gupta")
        initials = "".join(w[0] for w in name_parts).upper()
        if abbr_upper == initials:
            return name

        # 3. Upper-case initials only  (e.g. "RG" where middle words are lowercase)
        init_upper = "".join(w[0] for w in name_parts if w[0].isupper()).upper()
        if abbr_upper == init_upper and init_upper != initials:
            return name

        # 4. Prefix match
        if len(abbr) >= 2 and len(abbr_parts) == 1 and name_upper.startswith(abbr_upper):
            return name

        # 5. Last-name match
        if len(abbr_parts) == 1 and abbr_upper == name_parts[-1].upper():
            return name

        # 6. Last N words match
        if len(abbr_parts) >= 2 and len(name_parts) > len(abbr_parts):
            tail = " ".join(name_parts[-len(abbr_parts):]).upper()
            if abbr_upper == tail:
                return name

    return short_name

def build_ground_timezone_map(df_metadata):
    """
    Geocode all distinct cricket ground names to IANA timezone strings.

    Runs on the driver; results are broadcast as a small dict
    (typically fewer than 500 grounds).

    REQUIRES: geopy and timezonefinder installed as Pipeline cluster libraries.
    Falls back to empty dict (all times treated as UTC) if libraries are absent.

    Args:
        df_metadata: DataFrame containing a 'ground' column (full Bronze or Silver batch).

    Returns:
        dict  {ground_name: iana_timezone_string}
    """
    try:
        from geopy.geocoders import Nominatim
        from timezonefinder import TimezoneFinder

        geolocator = Nominatim(user_agent="cricket_dlt_pipeline", timeout=10)
        tf         = TimezoneFinder()

        grounds = [
            row["ground"]
            for row in df_metadata
                .select("ground")
                .distinct()
                .where("ground IS NOT NULL")
                .collect()
        ]

        tz_map = {}
        for ground in grounds:
            try:
                time.sleep(1.1)  # Nominatim rate-limit: 1 request / second
                location = geolocator.geocode(ground)
                if location:
                    tz = tf.timezone_at(lat=location.latitude, lng=location.longitude)
                    tz_map[ground] = tz or "UTC"
                else:
                    tz_map[ground] = "UTC"
            except Exception:
                tz_map[ground] = "UTC"

        return tz_map

    except ImportError:
        # Libraries not installed — all grounds default to UTC
        return {}


def apply_timezone_and_utc(df, tz_map):
    """
    Enrich a match_metadata DataFrame with:
      - local_timezone  (IANA string looked up from tz_map)
      - match_start_utc, first/second_innings_start/end_utc  (UTC timestamps)

    Args:
        df:     match_metadata DataFrame (must contain 'ground',
                'hours_of_play_local_time', 'match_date' columns).
        tz_map: dict returned by build_ground_timezone_map().

    Returns:
        DataFrame with timezone and UTC columns added.
    """
    if tz_map:
        tz_map_expr = create_map(
            *[x for k, v in tz_map.items() for x in (lit(k), lit(v))]
        )
        df = df.withColumn(
            "local_timezone",
            coalesce(tz_map_expr[col("ground")], lit("UTC"))
        )
    else:
        df = df.withColumn("local_timezone", lit("UTC"))

    time_cols = [
        ("start_time",           r"^(\d+\.\d+)\s+start",                   "match_start_utc"),
        ("first_innings_start",  r"First Session\s+(\d+\.\d+)",             "first_innings_start_utc"),
        ("first_innings_end",    r"First Session\s+\d+\.\d+-(\d+\.\d+)",    "first_innings_end_utc"),
        ("second_innings_start", r"Second Session\s+(\d+\.\d+)",            "second_innings_start_utc"),
        ("second_innings_end",   r"Second Session\s+\d+\.\d+-(\d+\.\d+)",   "second_innings_end_utc"),
    ]

    for temp_col, pattern, utc_col in time_cols:
        df = (
            df
            .withColumn(temp_col, trim(regexp_extract("hours_of_play_local_time", pattern, 1)))
            .withColumn(temp_col,
                when(col(temp_col) != "", to_timestamp(concat(col("match_date"), lit(" "), col(temp_col)), "yyyy-MM-dd HH.mm"))
                .otherwise(lit(None)))
            .withColumn(utc_col, to_utc_timestamp(col(temp_col), col("local_timezone")))
            .drop(temp_col)
        )

    return df
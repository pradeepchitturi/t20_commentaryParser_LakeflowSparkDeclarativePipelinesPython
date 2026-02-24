# Import modules
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField, LongType, BooleanType

from utilities.utils import build_ground_timezone_map, apply_timezone_and_utc, get_full_name

# COMMAND ----------
# DBTITLE 1,SILVER — match_players (Streaming Table)
# Dedup via dropDuplicates — DLT tracks seen keys across micro-batches
# using its internal state store, so duplicates are caught across runs too.

@dp.table(
    name=f"{CATALOG}.silver.match_players",
    comment="Deduplicated player records per match. Source: bronze.match_players.",
    table_properties={
        "quality":        "silver",
        "pipeline.layer": "silver"
    },
    partition_cols=["matchid"]
)
@dp.expect_all({
    "matchid_not_null":     "matchid IS NOT NULL",
    "player_name_not_null": "player_name IS NOT NULL AND player_name != ''",
    "team_not_null":        "team IS NOT NULL AND team != ''"
})
def match_players():
    return (
        dp.read_stream(f"{CATALOG}.bronze.match_players")
        # dropDuplicates() is stateful in DLT streaming — dedup is guaranteed
        # across pipeline runs, not just within one micro-batch
        .dropDuplicates(["matchid", "player_name", "team"])
        .select(
            "Batchid", "matchid", "innings", "team", "player_name",
            "batted", "batting_position", "player_type", "retired",
            "not_out", "bowled", "source_file", "load_timestamp",
            current_timestamp().alias("silver_load_timestamp")
        )
    )

# COMMAND ----------
# DBTITLE 1,SILVER — match_metadata (Streaming Table)
# Stateless per-row transforms: toss split, date parse, UTC times, team names.
#
# NOTE on build_ground_timezone_map():
#   - Called at driver level during each pipeline trigger
#   - Geocodes distinct grounds from the FULL Bronze table (dlt.read — batch)
#   - Results broadcast to workers as a create_map() column expression
#   - Streaming output is from dlt.read_stream() below

@dp.table(
    name=f"{CATALOG}.silver.match_metadata",
    comment="Enriched match metadata: toss split, UTC times, timezone, team name resolution.",
    table_properties={
        "quality":        "silver",
        "pipeline.layer": "silver"
    },
    partition_cols=["matchid"]
)
@dp.expect_all({
    "matchid_not_null":   "matchid IS NOT NULL",
    "series_not_null":    "series IS NOT NULL AND series != ''",
    "match_date_present": "match_date IS NOT NULL"
})
def match_metadata():
    # ── Timezone map ── built from ALL bronze grounds (batch, driver-side) ──
    df_bronze_full = dp.read(f"{CATALOG}.bronze.match_metadata")
    tz_map = build_ground_timezone_map(df_bronze_full)

    # ── Static player lookup for team abbreviation resolution ──
    df_players = dp.read(f"{CATALOG}.silver.match_players")
    df_match_team_names = (
        df_players.groupBy("matchid")
        .agg(collect_set("team").alias("team_names"))
    )

    # ── Streaming source: new Bronze metadata rows only ──
    df = dp.read_stream(f"{CATALOG}.bronze.match_metadata")

    # Dedup: one record per matchid
    df = df.dropDuplicates(["matchid"])

    # Toss split → toss_winner + decision
    toss_parts = split(col("toss"), ",")
    df = (
        df
        .withColumn("_parts",   toss_parts)
        .withColumn("toss",     trim(col("_parts")[0]))
        .withColumn("decision",
            when(size(col("_parts")) >= 2, trim(col("_parts")[1])).otherwise(lit(None)))
        .drop("_parts")
    )

    # Parse match_date from match_days  (e.g. "21 February 2026")
    df = df.withColumn("match_date",
        to_date(
            trim(regexp_extract("match_days", r"^(\d+\s+\w+\s+\d{4})", 1)),
            "d MMMM yyyy"
        )
    )

    # Apply timezone map → local_timezone + UTC time columns
    df = apply_timezone_and_utc(df, tz_map)

    # Resolve first/second innings team abbreviations (stream-static join)
    df = df.join(df_match_team_names, "matchid", "left")
    df = (
        df
        .withColumn("first_innings",  get_full_name("first_innings",  "team_names"))
        .withColumn("second_innings", get_full_name("second_innings", "team_names"))
    )

    return df.select(
        "Batchid", "matchid", "ground", "toss", "decision", "series", "season",
        "player_of_the_match", "player_of_the_series", "t20_debut", "t20i_debut",
        "umpires", "tv_umpire", "reserve_umpire", "match_referee",
        "points", "player_replacements", "match_number",
        "match_date", "match_start_utc",
        "first_innings_start_utc", "first_innings_end_utc",
        "second_innings_start_utc", "second_innings_end_utc",
        "first_innings", "second_innings",
        "has_super_over", "super_over_count", "series_result",
        "source_file", "load_timestamp",
        current_timestamp().alias("silver_load_timestamp")
    )


# COMMAND ----------
# DBTITLE 1,SILVER — match_events (Streaming Table)
#
# Stream-Static Join:
#   STREAMING  dlt.read_stream(bronze.match_events)  — new rows only
#   STATIC     dlt.read(silver.match_players)         — full table, re-read each trigger
#
# Stateless transforms applied per row (streaming-safe):
#   ✓ Event parsing  (Bowler, Batsman, runs_text from event string)
#   ✓ Runs calculation  (runs_text → integer, no-ball +1 penalty)
#   ✓ Extras classification  (normal / wide / noball / legbyes / byes)
#   ✓ Over & over_ball_number  (split from ball column)
#   ✓ Dismissal method + dismissed_by  (regex on commentary)
#   ✓ Name resolution  (get_full_name UDF via stream-static join)
#   ✓ Super Over backfill  (best-effort within each micro-batch)
#
# NOT computed here — requires Window.unboundedPreceding (stateful):
#   ✗ innings_score   → computed in gold.match_events (gold_aggregations.py)
#   ✗ wickets_lost    → computed in gold.match_events (gold_aggregations.py)

@dlt.table(
    name=f"{CATALOG}.silver.match_events",
    comment="Streaming: runs, dismissals, name resolution. innings_score/wickets_lost in gold.match_events.",
    table_properties={
        "quality":        "silver",
        "pipeline.layer": "silver"
    },
    partition_cols=["matchid"]
)
@dlt.expect_all({
    "matchid_not_null":    "matchid IS NOT NULL",
    "runs_not_null":       "runs IS NOT NULL",
    "runs_valid_range":    "runs >= 0 AND runs <= 7",
    "over_not_null":       "over IS NOT NULL",
    "extras_valid_domain": "Extras IN ('normal','wide','noball','legbyes','byes')",
    "dismissal_valid":     "dismissal_method IN ('Not Out','Caught','Bowled','LBW','Run Out','Stumped','Caught & Bowled')"
})
def match_events():
    # ── Static: full Silver players table for name lookup ──────────────────
    df_players = dlt.read(f"{CATALOG}.silver.match_players")
    df_match_names = (
        df_players.groupBy("matchid")
        .agg(
            collect_set("team").alias("team_names"),
            collect_set("player_name").alias("player_names")
        )
    )

    # ── Streaming: new Bronze events only ──────────────────────────────────
    df = dlt.read_stream(f"{CATALOG}.bronze.match_events")

    # ── Event column parsing: "Bowler to Batsman, Score" ───────────────────
    EVENT_PATTERN = r"([A-Za-z\s\'\\-]*)\sto\s([A-Za-z\s\'\\-]*)\,\s+([0-9A-Za-z\,\s\(\)]*)"
    df = (
        df
        .withColumn("Bowler",    trim(regexp_extract("event", EVENT_PATTERN, 1)))
        .withColumn("Batsman",   trim(regexp_extract("event", EVENT_PATTERN, 2)))
        .withColumn("runs_text", trim(regexp_replace(
            trim(regexp_extract("event", EVENT_PATTERN, 3)), r"\(no ball\)", "")))
        .withColumn("Extras",
            when(col("runs_text").contains("wide"),     "wide")
            .when(col("event").contains("no ball"),     "noball")
            .when(col("runs_text").contains("leg bye"), "legbyes")
            .when(col("runs_text").contains("bye"),     "byes")
            .otherwise("normal"))
        .withColumn("over",
            split(col("ball").cast("string"), "\\.")[0].cast("int") + 1)
        .withColumn("over_ball_number",
            split(col("ball").cast("string"), "\\.")[1].cast("int"))
        .withColumn("runs",
            when(col("runs_text") == "OUT",          0)
            .when(col("runs_text").isin("no run",""), 0)
            .when(col("runs_text") == "FOUR runs",   4)
            .when(col("runs_text") == "SIX runs",    6)
            .when(col("runs_text").rlike(r"^\d+"),
                  regexp_extract("runs_text", r"^(\d+)", 1).cast("int"))
            .otherwise(0))
        .withColumn("runs",
            when(col("Extras") == "noball", col("runs") + 1).otherwise(col("runs")))
    )

    # ── Dismissal extraction (stateless regex per row) ──────────────────────
    is_out = col("runs_text").contains("OUT")
    comm   = col("commentary")
    PAT_RUNOUT  = r"run out\s*\("
    PAT_CANDB   = r"c & b\s+"
    PAT_CAUGHT  = r"\bc\s+.+?\s+b\s+.+?\s+\d+\s*\("
    PAT_LBW     = r"lbw\s+b\s+"
    PAT_STUMPED = r"(?:^|[^a-zA-Z])st\s+[^a-zA-Z]*[A-Z].+?\s+b\s+.+?\s+\d+\s*\("
    PAT_BOWLED  = r"(?:^|[^a-zA-Z])b\s+\w+\s+\d+\s*\("

    df = (
        df
        .withColumn("dismissal_method",
            when(~is_out,                  "Not Out")
            .when(comm.rlike(PAT_RUNOUT),  "Run Out")
            .when(comm.rlike(PAT_CANDB),   "Caught & Bowled")
            .when(comm.rlike(PAT_CAUGHT),  "Caught")
            .when(comm.rlike(PAT_LBW),     "LBW")
            .when(comm.rlike(PAT_STUMPED), "Stumped")
            .when(comm.rlike(PAT_BOWLED),  "Bowled")
            .otherwise("Not Out"))
        .withColumn("dismissed_by",
            when(~is_out, lit(None))
            .when(comm.rlike(PAT_RUNOUT),  regexp_extract("commentary", r"run out\s*\(([^/\)]+)",  1))
            .when(comm.rlike(PAT_CANDB),   regexp_extract("commentary", r"c & b\s+(.+?)(?=\s+\d+\s*\()", 1))
            .when(comm.rlike(PAT_CAUGHT),  regexp_extract("commentary", r"\bc\s+(.+?)\s+b\s+",    1))
            .when(comm.rlike(PAT_LBW),     regexp_extract("commentary", r"lbw\s+b\s+(.+?)(?=\s+\d+\s*\()", 1))
            .when(comm.rlike(PAT_STUMPED), regexp_extract("commentary", r"st\s+(.+?)\s+b\s+",     1))
            .when(comm.rlike(PAT_BOWLED),  regexp_extract("commentary", r"(?:^|[^a-zA-Z])b\s+(.+?)(?=\s+\d+\s*\()", 1))
            .otherwise(lit(None)))
        .withColumn("dismissed_by", trim(regexp_replace("dismissed_by", "†", "")))
    )

    # ── Name resolution: stream-static join ────────────────────────────────
    df = df.join(df_match_names, "matchid", "left")
    df = (
        df
        .withColumn("team",         get_full_name("innings",  "team_names"))
        .withColumn("batsman_full", get_full_name("Batsman",  "player_names"))
        .withColumn("bowler_full",  get_full_name("Bowler",   "player_names"))
    )

    # ── Super Over flag (stateless per-row — no streaming self-join) ────────
    df = df.withColumn("super_over_team",
        when(col("team").contains("Super Over"), col("team")).otherwise(lit("normal")))

    # ── Final select — NO innings_score, NO wickets_lost ───────────────────
    return df.select(
        "Batchid", "matchid", "event", "ball", "over", "over_ball_number", "match_ball_number",
        "runs", "commentary",
        col("bowler_full").alias("bowler"),
        col("batsman_full").alias("batsman"),
        "team", "is_super_over", "super_over_team",
        "runs_text", "Extras",
        "dismissal_method", "dismissed_by",
        "source_file", "load_timestamp",
        current_timestamp().alias("silver_load_timestamp")
    )
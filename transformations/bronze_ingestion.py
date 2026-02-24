# Import modules
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField, LongType, BooleanType
from utilities import utils

CATALOG = spark.conf.get("pipeline.catalog", "T20_catalog_dev_dlt") 

# Override defaults with DLT pipeline configuration values
utils.SOURCE_PATH = spark.conf.get("pipeline.source_path", utils.SOURCE_PATH)
utils.SCHEMA_BASE = spark.conf.get("pipeline.schema_location_base", utils.SCHEMA_BASE)

# COMMAND ----------
# DBTITLE 1,BRONZE — match_events (ball-by-ball CSV)

@dp.table(
    name=f"{CATALOG}.bronze.match_events",
    comment="Raw ball-by-ball match events ingested from ADLS CSV files via Auto Loader.",
    table_properties={
        "quality":                    "bronze",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["matchid"]
)
@dp.expect_all({
    "matchid_not_null":    "matchid IS NOT NULL",
    "matchid_positive":    "matchid > 0",
    "ball_not_null":       "ball IS NOT NULL AND ball != ''",
    "innings_not_null":    "innings IS NOT NULL AND innings != ''",
    "source_file_present": "source_file IS NOT NULL"
})
def match_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",           "csv")
        .option("cloudFiles.schemaLocation",   f"{utils.SCHEMA_BASE}/bronze/match_events")
        .option("cloudFiles.schemaHints",      "matchid BIGINT, match_ball_number BIGINT, is_super_over BOOLEAN")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header",                      "true")
        .load(f"{utils.SOURCE_PATH}cricket_commentary/matchid=*/match_events_data.csv")
        .select(
            lit(spark.conf.get("pipeline.run_id", "dlt")).alias("Batchid"),
            col("match_ball_number").cast(LongType()),
            col("ball").cast("string"),
            col("event").cast("string"),
            col("commentary").cast("string"),
            col("bowler").cast("string"),
            col("batsman").cast("string"),
            col("innings").cast("string"),
            col("matchid").cast(LongType()),
            col("is_super_over").cast(BooleanType()),
            col("_metadata.file_path").alias("source_file"),
            current_timestamp().alias("load_timestamp")
        )
    )


# COMMAND ----------
# DBTITLE 1,BRONZE — match_metadata (match-level JSON)

@dp.table(
    name=f"{CATALOG}.bronze.match_metadata",
    comment="Raw match metadata (venue, toss, result, officials) from ADLS JSON files.",
    table_properties={
        "quality":                    "bronze",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["matchid"]
)
@dp.expect_all({
    "matchid_not_null": "matchid IS NOT NULL",
    "matchid_positive": "matchid > 0",
    "series_not_null":  "series IS NOT NULL AND series != ''"
})
def match_metadata():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",           "json")
        .option("cloudFiles.schemaLocation",   f"{utils.SCHEMA_BASE}/bronze/match_metadata")
        .option("cloudFiles.schemaHints",      "matchid BIGINT, has_super_over BOOLEAN, super_over_count INT")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{utils.SOURCE_PATH}cricket_commentary/matchid=*/metadata_data.json")
        .select(
            lit(spark.conf.get("pipeline.run_id", "dlt")).alias("Batchid"),
            col("ground").cast("string"),
            col("toss").cast("string"),
            col("series").cast("string"),
            col("season").cast("string"),
            col("player_of_the_match").cast("string"),
            col("player_of_the_series").cast("string"),
            col("hours_of_play_local_time").cast("string"),
            col("match_days").cast("string"),
            col("t20_debut").cast("string"),
            col("t20i_debut").cast("string"),
            col("umpires").cast("string"),
            col("tv_umpire").cast("string"),
            col("reserve_umpire").cast("string"),
            col("match_referee").cast("string"),
            col("points").cast("string"),
            col("match_number").cast("string"),
            col("matchid").cast(LongType()),
            col("player_replacements").cast("string"),
            col("first_innings").cast("string"),
            col("second_innings").cast("string"),
            col("has_super_over").cast(BooleanType()),
            col("super_over_count").cast(IntegerType()),
            col("series_result").cast("string"),
            col("_metadata.file_path").alias("source_file"),
            current_timestamp().alias("load_timestamp")
        )
    )


# COMMAND ----------
# DBTITLE 1,BRONZE — match_players (player-level CSV)

@dp.table(
    name=f"{CATALOG}.bronze.match_players",
    comment="Raw player lineup records per match from ADLS CSV files.",
    table_properties={
        "quality":                    "bronze",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["matchid"]
)
@dp.expect_all({
    "matchid_not_null":     "matchid IS NOT NULL",
    "matchid_positive":     "matchid > 0",
    "player_name_not_null": "player_name IS NOT NULL AND player_name != ''",
    "team_not_null":        "team IS NOT NULL AND team != ''"
})
def match_players():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",           "csv")
        .option("cloudFiles.schemaLocation",   f"{utils.SCHEMA_BASE}/bronze/match_players")
        .option("cloudFiles.schemaHints",      "matchid BIGINT, batted BOOLEAN, batting_position INT")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header",                      "true")
        .load(f"{utils.SOURCE_PATH}cricket_commentary/matchid=*/match_players_data.csv")
        .select(
            lit(spark.conf.get("pipeline.run_id", "dlt")).alias("Batchid"),
            col("matchid").cast(LongType()),
            col("innings").cast("string"),
            col("team").cast("string"),
            col("player_name").cast("string"),
            col("batted").cast(BooleanType()),
            col("batting_position").cast(IntegerType()),
            col("player_type").cast("string"),
            col("retired").cast("string"),
            col("not_out").cast("string"),
            col("bowled").cast("string"),
            col("_metadata.file_path").alias("source_file"),
            current_timestamp().alias("load_timestamp")
        )
    )
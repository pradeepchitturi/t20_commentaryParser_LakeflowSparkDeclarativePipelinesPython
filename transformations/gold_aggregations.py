# Import modules
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, when, regexp_extract, regexp_replace, trim,
    collect_set, lit, current_timestamp,
    sum as spark_sum, count, countDistinct,
    min as _min, max as _max, floor, round as _round, lower,
    row_number, lead
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField, LongType, BooleanType
from pyspark.sql.window import Window


# COMMAND ----------
# DBTITLE 1,GOLD — gold_match_events (Materialized View)

@dp.table(
    name=f"{CATALOG}.gold.match_events",
    comment="silver_match_events enriched with innings_score + wickets_lost (window sums).",
    table_properties={
        "quality":        "gold",
        "pipeline.layer": "gold"
    },
    partition_cols=["matchid"]
)
@dp.expect_all({
    "innings_score_non_negative": "innings_score >= 0",
    "wickets_range":              "wickets_lost >= 0 AND wickets_lost <= 10"
})
def match_events():
    df = dp.read(f"{CATALOG}.silver.match_events")

    innings_window = (
        Window
        .partitionBy("matchid", "team", "super_over_team")
        .orderBy("over", "match_ball_number")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        df
        .withColumn("innings_score",
            spark_sum("runs").over(innings_window))
        .withColumn("wickets_lost",
            spark_sum(when(col("dismissal_method") != "Not Out", 1).otherwise(0))
            .over(innings_window))
        .select(
            "Batchid", "matchid", "event", "ball", "over", "over_ball_number", "match_ball_number",
            "runs", "innings_score", "wickets_lost", "commentary",
            "bowler", "batsman", "team", "is_super_over", "super_over_team",
            "runs_text", "Extras", "dismissal_method", "dismissed_by",
            "source_file", "load_timestamp", "silver_load_timestamp",
            current_timestamp().alias("gold_load_timestamp")
        )
    )


# COMMAND ----------
# DBTITLE 1,GOLD — gold_batting_stats_per_match

@dp.table(
    name=f"{CATALOG}.gold.batting_stats_per_match",
    comment="Batting stats per batsman per match: runs, balls faced, SR, run distribution.",
    table_properties={"quality": "gold", "pipeline.layer": "gold"}
)
def gold_batting_stats_per_match():
    df_events   = dp.read(f"{CATALOG}.gold.match_events")
    df_metadata = dp.read(f"{CATALOG}.silver.match_metadata")
    df = df_events.join(
        df_metadata.select("matchid", "series", "season"), "matchid", "left"
    )

    df = (
        df
        .withColumn("batsman_runs",
            when(col("Extras").isin("legbyes", "byes", "wide"), lit(0))
            .otherwise(col("runs")))
        .withColumn("ball_faced",
            when(col("Extras") == "wide", lit(0)).otherwise(lit(1)))
    )

    df_dismissals = (
        df.groupBy("matchid", "batsman", "team")
        .agg(
            spark_sum(when(col("dismissal_method") != "Not Out", lit(1)).otherwise(lit(0)))
            .alias("times_dismissed")
        )
    )

    df_agg = (
        df.groupBy("matchid", "batsman", "team", "series", "season")
        .agg(
            spark_sum("ball_faced").alias("balls_faced"),
            spark_sum("batsman_runs").alias("total_runs"),
            spark_sum(when(col("batsman_runs") == 0, 1).otherwise(0)).alias("dot_balls"),
            spark_sum(when(col("batsman_runs") == 1, 1).otherwise(0)).alias("runs_1s"),
            spark_sum(when(col("batsman_runs") == 2, 1).otherwise(0)).alias("runs_2s"),
            spark_sum(when(col("batsman_runs") == 3, 1).otherwise(0)).alias("runs_3s"),
            spark_sum(when(col("batsman_runs") == 4, 1).otherwise(0)).alias("runs_4s"),
            spark_sum(when(col("batsman_runs") == 5, 1).otherwise(0)).alias("runs_5s"),
            spark_sum(when(col("batsman_runs") >= 6, 1).otherwise(0)).alias("runs_6s")
        )
    )

    return (
        df_agg.join(df_dismissals, ["matchid", "batsman", "team"], "left")
        .withColumn("not_out",
            when(col("times_dismissed") == 0, lit(True)).otherwise(lit(False)))
        .withColumn("strike_rate",
            when(col("balls_faced") > 0,
                 _round((col("total_runs") / col("balls_faced")) * 100, 2))
            .otherwise(lit(0.0)))
        .withColumn("gold_load_timestamp", current_timestamp())
        .select(
            "matchid", "series", "season", "batsman", "team",
            "balls_faced", "total_runs", "strike_rate",
            "dot_balls", "runs_1s", "runs_2s", "runs_3s", "runs_4s", "runs_5s", "runs_6s",
            "not_out", "times_dismissed", "gold_load_timestamp"
        )
    )


# COMMAND ----------
# DBTITLE 1,GOLD — gold_batting_stats_per_series

@dp.table(
    name=f"{CATALOG}.gold.batting_stats_per_series",
    comment="Batting stats per batsman per series: milestone counts, batting average.",
    table_properties={"quality": "gold", "pipeline.layer": "gold"}
)
def gold_batting_stats_per_series():
    df = dp.read(f"{CATALOG}.gold.batting_stats_per_match")
    return (
        df.groupBy("series", "season", "batsman")
        .agg(
            countDistinct("matchid").alias("matches_played"),
            spark_sum("balls_faced").alias("total_balls_faced"),
            spark_sum("total_runs").alias("total_runs"),
            spark_sum("dot_balls").alias("dot_balls"),
            spark_sum("runs_1s").alias("runs_1s"),
            spark_sum("runs_2s").alias("runs_2s"),
            spark_sum("runs_3s").alias("runs_3s"),
            spark_sum("runs_4s").alias("runs_4s"),
            spark_sum("runs_5s").alias("runs_5s"),
            spark_sum("runs_6s").alias("runs_6s"),
            spark_sum(when(col("total_runs") >= 30,  1).otherwise(0)).alias("scores_gte_30"),
            spark_sum(when(col("total_runs") >= 50,  1).otherwise(0)).alias("scores_gte_50"),
            spark_sum(when(col("total_runs") >= 100, 1).otherwise(0)).alias("scores_gte_100"),
            spark_sum(when(col("not_out") == True,   1).otherwise(0)).alias("not_out_count"),
            spark_sum("times_dismissed").alias("total_dismissals")
        )
        .withColumn("strike_rate",
            when(col("total_balls_faced") > 0,
                 _round((col("total_runs") / col("total_balls_faced")) * 100, 2))
            .otherwise(lit(0.0)))
        .withColumn("batting_average",
            when(col("total_dismissals") > 0,
                 _round(col("total_runs") / col("total_dismissals"), 2))
            .otherwise(col("total_runs").cast("double")))
        .withColumn("gold_load_timestamp", current_timestamp())
        .select(
            "series", "season", "batsman", "matches_played",
            "total_balls_faced", "total_runs", "strike_rate", "batting_average",
            "dot_balls", "runs_1s", "runs_2s", "runs_3s", "runs_4s", "runs_5s", "runs_6s",
            "scores_gte_30", "scores_gte_50", "scores_gte_100",
            "not_out_count", "total_dismissals", "gold_load_timestamp"
        )
    )

# COMMAND ----------
# DBTITLE 1,GOLD — gold_batting_stats_overall

@dp.table(
    name=f"{CATALOG}.gold.batting_stats_overall",
    comment="Career batting stats per batsman across all series.",
    table_properties={"quality": "gold", "pipeline.layer": "gold"}
)
def gold_batting_stats_overall():
    df = dp.read(f"{CATALOG}.gold.batting_stats_per_series")
    return (
        df.groupBy("batsman")
        .agg(
            countDistinct("series").alias("series_played"),
            spark_sum("matches_played").alias("total_matches"),
            spark_sum("total_balls_faced").alias("total_balls_faced"),
            spark_sum("total_runs").alias("total_runs"),
            spark_sum("dot_balls").alias("dot_balls"),
            spark_sum("runs_1s").alias("runs_1s"),
            spark_sum("runs_2s").alias("runs_2s"),
            spark_sum("runs_3s").alias("runs_3s"),
            spark_sum("runs_4s").alias("runs_4s"),
            spark_sum("runs_5s").alias("runs_5s"),
            spark_sum("runs_6s").alias("runs_6s"),
            spark_sum("scores_gte_30").alias("scores_gte_30"),
            spark_sum("scores_gte_50").alias("scores_gte_50"),
            spark_sum("scores_gte_100").alias("scores_gte_100"),
            spark_sum("not_out_count").alias("not_out_count"),
            spark_sum("total_dismissals").alias("total_dismissals")
        )
        .withColumn("strike_rate",
            when(col("total_balls_faced") > 0,
                 _round((col("total_runs") / col("total_balls_faced")) * 100, 2))
            .otherwise(lit(0.0)))
        .withColumn("batting_average",
            when(col("total_dismissals") > 0,
                 _round(col("total_runs") / col("total_dismissals"), 2))
            .otherwise(col("total_runs").cast("double")))
        .withColumn("gold_load_timestamp", current_timestamp())
        .select(
            "batsman", "series_played", "total_matches",
            "total_balls_faced", "total_runs", "strike_rate", "batting_average",
            "dot_balls", "runs_1s", "runs_2s", "runs_3s", "runs_4s", "runs_5s", "runs_6s",
            "scores_gte_30", "scores_gte_50", "scores_gte_100",
            "not_out_count", "total_dismissals", "gold_load_timestamp"
        )
    )

# COMMAND ----------
# DBTITLE 1,GOLD — gold_bowling_stats_per_match

@dp.table(
    name=f"{CATALOG}.gold.bowling_stats_per_match",
    comment="Bowling stats per bowler per match. Legbyes/byes excluded from runs conceded.",
    table_properties={"quality": "gold", "pipeline.layer": "gold"}
)
def gold_bowling_stats_per_match():
    df_events   = dp.read(f"{CATALOG}.gold.match_events")
    df_metadata = dp.read(f"{CATALOG}.silver.match_metadata")
    df = (
        df_events.filter(col("bowler").isNotNull())
        .join(df_metadata.select("matchid", "series", "season"), "matchid", "left")
    )

    df = (
        df
        .withColumn("is_legal_ball",
            when(col("Extras").isin("wide", "noball"), lit(0)).otherwise(lit(1)))
        .withColumn("bowler_runs",
            when(col("Extras").isin("legbyes", "byes"), lit(0)).otherwise(col("runs")))
        .withColumn("is_dot_ball",
            when((col("is_legal_ball") == 1) & (col("bowler_runs") == 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("is_wicket",
            when(
                (col("dismissal_method") != "Not Out") &
                (col("dismissal_method") != "Run Out"),
                lit(1)
            ).otherwise(lit(0)))
    )

    df_over = (
        df.groupBy("matchid", "bowler", "over", "series", "season")
        .agg(
            spark_sum("is_legal_ball").alias("legal_balls_in_over"),
            spark_sum("bowler_runs").alias("bowler_runs_in_over")
        )
    )
    df_maiden = (
        df_over.groupBy("matchid", "bowler", "series", "season")
        .agg(
            spark_sum(
                when(
                    (col("legal_balls_in_over") == 6) & (col("bowler_runs_in_over") == 0),
                    lit(1)
                ).otherwise(lit(0))
            ).alias("maiden_overs")
        )
    )

    df_bowl = (
        df.groupBy("matchid", "bowler", "series", "season")
        .agg(
            spark_sum("bowler_runs").alias("runs_conceded"),
            count("*").alias("total_deliveries"),
            spark_sum("is_legal_ball").alias("legal_balls_bowled"),
            spark_sum("is_dot_ball").alias("dot_balls"),
            spark_sum("is_wicket").alias("wickets_taken"),
            spark_sum(when(col("bowler_runs") == 1, 1).otherwise(0)).alias("runs_1s_conceded"),
            spark_sum(when(col("bowler_runs") == 2, 1).otherwise(0)).alias("runs_2s_conceded"),
            spark_sum(when(col("bowler_runs") == 3, 1).otherwise(0)).alias("runs_3s_conceded"),
            spark_sum(when(col("bowler_runs") == 4, 1).otherwise(0)).alias("runs_4s_conceded"),
            spark_sum(when(col("bowler_runs") == 5, 1).otherwise(0)).alias("runs_5s_conceded"),
            spark_sum(when(col("bowler_runs") >= 6, 1).otherwise(0)).alias("runs_6s_conceded")
        )
    )

    return (
        df_bowl.join(df_maiden, ["matchid", "bowler", "series", "season"], "left")
        .withColumn("overs_bowled",
            _round(
                floor(col("legal_balls_bowled") / 6) +
                (col("legal_balls_bowled") % 6) / 10, 1
            ))
        .withColumn("economy_rate",
            when(col("legal_balls_bowled") > 0,
                 _round(col("runs_conceded") / (col("legal_balls_bowled") / 6), 2))
            .otherwise(lit(0.0)))
        .withColumn("gold_load_timestamp", current_timestamp())
        .select(
            "matchid", "series", "season", "bowler",
            "total_deliveries", "legal_balls_bowled", "overs_bowled",
            "runs_conceded", "economy_rate", "maiden_overs", "dot_balls",
            "runs_1s_conceded", "runs_2s_conceded", "runs_3s_conceded",
            "runs_4s_conceded", "runs_5s_conceded", "runs_6s_conceded",
            "wickets_taken", "gold_load_timestamp"
        )
    )

# COMMAND ----------
# DBTITLE 1,GOLD — gold_bowling_stats_per_series

@dp.table(
    name=f"{CATALOG}.gold.bowling_stats_per_series",
    comment="Bowling stats per bowler per series, including 5-wicket haul counts.",
    table_properties={"quality": "gold", "pipeline.layer": "gold"}
)
def gold_bowling_stats_per_series():
    df = dp.read(f"{CATALOG}.gold.bowling_stats_per_match")
    return (
        df.groupBy("series", "season", "bowler")
        .agg(
            countDistinct("matchid").alias("matches_played"),
            spark_sum("total_deliveries").alias("total_deliveries"),
            spark_sum("legal_balls_bowled").alias("total_legal_balls"),
            spark_sum("runs_conceded").alias("total_runs_conceded"),
            spark_sum("maiden_overs").alias("total_maiden_overs"),
            spark_sum("dot_balls").alias("total_dot_balls"),
            spark_sum("wickets_taken").alias("total_wickets"),
            spark_sum("runs_1s_conceded").alias("runs_1s_conceded"),
            spark_sum("runs_2s_conceded").alias("runs_2s_conceded"),
            spark_sum("runs_3s_conceded").alias("runs_3s_conceded"),
            spark_sum("runs_4s_conceded").alias("runs_4s_conceded"),
            spark_sum("runs_5s_conceded").alias("runs_5s_conceded"),
            spark_sum("runs_6s_conceded").alias("runs_6s_conceded"),
            spark_sum(when(col("wickets_taken") >= 5, 1).otherwise(0)).alias("five_wicket_hauls")
        )
        .withColumn("total_overs_bowled",
            _round(
                floor(col("total_legal_balls") / 6) +
                (col("total_legal_balls") % 6) / 10, 1
            ))
        .withColumn("economy_rate",
            when(col("total_legal_balls") > 0,
                 _round(col("total_runs_conceded") / (col("total_legal_balls") / 6), 2))
            .otherwise(lit(0.0)))
        .withColumn("bowling_average",
            when(col("total_wickets") > 0,
                 _round(col("total_runs_conceded") / col("total_wickets"), 2))
            .otherwise(lit(None).cast("double")))
        .withColumn("bowling_strike_rate",
            when(col("total_wickets") > 0,
                 _round(col("total_legal_balls") / col("total_wickets"), 2))
            .otherwise(lit(None).cast("double")))
        .withColumn("gold_load_timestamp", current_timestamp())
        .select(
            "series", "season", "bowler", "matches_played",
            "total_deliveries", "total_legal_balls", "total_overs_bowled",
            "total_runs_conceded", "economy_rate", "bowling_average", "bowling_strike_rate",
            "total_maiden_overs", "total_dot_balls",
            "runs_1s_conceded", "runs_2s_conceded", "runs_3s_conceded",
            "runs_4s_conceded", "runs_5s_conceded", "runs_6s_conceded",
            "total_wickets", "five_wicket_hauls", "gold_load_timestamp"
        )
    )

# COMMAND ----------
# DBTITLE 1,GOLD — gold_bowling_stats_overall

@dp.table(
    name=f"{CATALOG}.gold.bowling_stats_overall",
    comment="Career bowling stats per bowler across all series.",
    table_properties={"quality": "gold", "pipeline.layer": "gold"}
)
def gold_bowling_stats_overall():
    df = dp.read(f"{CATALOG}.gold.bowling_stats_per_series")
    return (
        df.groupBy("bowler")
        .agg(
            countDistinct("series").alias("series_played"),
            spark_sum("matches_played").alias("total_matches"),
            spark_sum("total_deliveries").alias("total_deliveries"),
            spark_sum("total_legal_balls").alias("total_legal_balls"),
            spark_sum("total_runs_conceded").alias("total_runs_conceded"),
            spark_sum("total_maiden_overs").alias("total_maiden_overs"),
            spark_sum("total_dot_balls").alias("total_dot_balls"),
            spark_sum("total_wickets").alias("total_wickets"),
            spark_sum("runs_1s_conceded").alias("runs_1s_conceded"),
            spark_sum("runs_2s_conceded").alias("runs_2s_conceded"),
            spark_sum("runs_3s_conceded").alias("runs_3s_conceded"),
            spark_sum("runs_4s_conceded").alias("runs_4s_conceded"),
            spark_sum("runs_5s_conceded").alias("runs_5s_conceded"),
            spark_sum("runs_6s_conceded").alias("runs_6s_conceded"),
            spark_sum("five_wicket_hauls").alias("five_wicket_hauls")
        )
        .withColumn("total_overs_bowled",
            _round(
                floor(col("total_legal_balls") / 6) +
                (col("total_legal_balls") % 6) / 10, 1
            ))
        .withColumn("economy_rate",
            when(col("total_legal_balls") > 0,
                 _round(col("total_runs_conceded") / (col("total_legal_balls") / 6), 2))
            .otherwise(lit(0.0)))
        .withColumn("bowling_average",
            when(col("total_wickets") > 0,
                 _round(col("total_runs_conceded") / col("total_wickets"), 2))
            .otherwise(lit(None).cast("double")))
        .withColumn("bowling_strike_rate",
            when(col("total_wickets") > 0,
                 _round(col("total_legal_balls") / col("total_wickets"), 2))
            .otherwise(lit(None).cast("double")))
        .withColumn("gold_load_timestamp", current_timestamp())
        .select(
            "bowler", "series_played", "total_matches",
            "total_deliveries", "total_legal_balls", "total_overs_bowled",
            "total_runs_conceded", "economy_rate", "bowling_average", "bowling_strike_rate",
            "total_maiden_overs", "total_dot_balls",
            "runs_1s_conceded", "runs_2s_conceded", "runs_3s_conceded",
            "runs_4s_conceded", "runs_5s_conceded", "runs_6s_conceded",
            "total_wickets", "five_wicket_hauls", "gold_load_timestamp"
        )
    )

# COMMAND ----------
# DBTITLE 1,GOLD — gold_match_summary

@dp.table(
    name=f"{CATALOG}.gold.match_summary",
    comment="Match-level summary: teams, venue, toss, winner, officials.",
    table_properties={"quality": "gold", "pipeline.layer": "gold"}
)
def gold_match_summary():
    df = dp.read(f"{CATALOG}.silver.match_metadata")
    return (
        df
        .withColumn("winner",
            regexp_extract(col("series_result"), r"^(.+?)\s+won\s+by", 1))
        .withColumn("win_margin",
            regexp_extract(col("series_result"), r"won\s+by\s+(.+)$", 1))
        .withColumn("result_type",
            when(lower(col("series_result")).contains("run"),         lit("by runs"))
            .when(lower(col("series_result")).contains("wicket"),     lit("by wickets"))
            .when(lower(col("series_result")).contains("super over"), lit("super over"))
            .when(lower(col("series_result")).contains("no result"),  lit("no result"))
            .when(lower(col("series_result")).contains("tie"),        lit("tie"))
            .otherwise(lit("other")))
        .select(
            "matchid", "match_number", "series", "season",
            col("first_innings").alias("team_1"),
            col("second_innings").alias("team_2"),
            col("ground").alias("venue"),
            "match_date", "match_start_utc",
            col("toss").alias("toss_winner"),
            col("decision").alias("toss_decision"),
            col("series_result").alias("result_text"),
            "winner", "win_margin", "result_type",
            "has_super_over", "super_over_count",
            "player_of_the_match", "umpires", "tv_umpire", "match_referee", "points",
            current_timestamp().alias("gold_load_timestamp")
        )
    )

# COMMAND ----------
# DBTITLE 1,GOLD — gold_player_team_scd2 (SCD Type 2)

@dp.table(
    name=f"{CATALOG}.gold.player_team_scd2",
    comment="SCD Type 2: player-team relationship with start/end dates and active flag.",
    table_properties={"quality": "gold", "pipeline.layer": "gold"}
)
def gold_player_team_scd2():
    df_events   = dp.read(f"{CATALOG}.gold.match_events")
    df_metadata = dp.read(f"{CATALOG}.silver.match_metadata")
    df_players  = dp.read(f"{CATALOG}.silver.match_players")

    df_bat_app = (
        df_events
        .select(col("matchid"), col("batsman").alias("player_name"), col("team"))
        .distinct()
    )
    df_player_app = df_players.select("matchid", "player_name", "team").distinct()

    df_all = (
        df_bat_app.unionByName(df_player_app)
        .distinct()
        .join(df_metadata.select("matchid", "match_date"), "matchid", "left")
        .filter(col("match_date").isNotNull())
    )

    df_spans = (
        df_all.groupBy("player_name", "team")
        .agg(
            _min("match_date").alias("first_match_date"),
            _max("match_date").alias("last_match_date"),
            countDistinct("matchid").alias("matches_for_team")
        )
    )

    scd_w      = Window.partitionBy("player_name").orderBy("first_match_date")
    scd_w_desc = Window.partitionBy("player_name").orderBy(col("first_match_date").desc())

    return (
        df_spans
        .withColumn("row_num",         row_number().over(scd_w))
        .withColumn("latest_row",      row_number().over(scd_w_desc))
        .withColumn("next_team_start", lead("first_match_date").over(scd_w))
        .withColumn("start_date",      col("first_match_date"))
        .withColumn("end_date",
            when(col("next_team_start").isNotNull(), col("next_team_start"))
            .otherwise(lit(None).cast("date")))
        .withColumn("is_active",
            when(col("latest_row") == 1, lit(True)).otherwise(lit(False)))
        .select(
            "player_name", "team", "start_date", "end_date",
            "is_active", "matches_for_team",
            current_timestamp().alias("gold_load_timestamp")
        )
    )

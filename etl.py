"""Data lake ETL with PySpark: raw JSON in, clean partitioned Parquet out.

Local (reads ../data, writes ./output):
    python etl.py --input ../data --output ./output

Local against S3 (downloads the matching hadoop-aws connector on first run):
    python etl.py --input s3://<raw-bucket>/raw --output s3://<lake-bucket>/curated --local-s3

On Amazon EMR Serverless (see scripts/run_emr_serverless.sh):
    spark-submit etl.py --input s3://<raw-bucket>/raw --output s3://<lake-bucket>/curated

Layout produced under --output (Hive-style partitions, readable by Athena,
Redshift Spectrum, Glue, DuckDB, pandas...):

    songs/year=1984/part-*.parquet
    artists/part-*.parquet
    users/part-*.parquet
    time/year=2026/month=9/part-*.parquet
    songplays/year=2026/month=9/part-*.parquet
    _quarantine/log_data/...            malformed input lines, kept for inspection

The code sticks to Python 3.9 syntax and DataFrame APIs available in Spark 3.5,
because that's what EMR Serverless release emr-7.x runs.
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -------------------------------------------------------------- schemas
# Declaring schemas instead of letting Spark infer them is faster (no extra
# pass over the data) and safer (a bad file can't silently change a type).

SONG_SCHEMA = T.StructType([
    T.StructField("num_songs", T.IntegerType()),
    T.StructField("artist_id", T.StringType()),
    T.StructField("artist_latitude", T.DoubleType()),
    T.StructField("artist_longitude", T.DoubleType()),
    T.StructField("artist_location", T.StringType()),
    T.StructField("artist_name", T.StringType()),
    T.StructField("song_id", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("duration", T.DoubleType()),
    T.StructField("year", T.IntegerType()),
])

LOG_SCHEMA = T.StructType([
    T.StructField("artist", T.StringType()),
    T.StructField("auth", T.StringType()),
    T.StructField("firstName", T.StringType()),
    T.StructField("gender", T.StringType()),
    T.StructField("itemInSession", T.LongType()),
    T.StructField("lastName", T.StringType()),
    T.StructField("length", T.DoubleType()),
    T.StructField("level", T.StringType()),
    T.StructField("location", T.StringType()),
    T.StructField("method", T.StringType()),
    T.StructField("page", T.StringType()),
    T.StructField("registration", T.DoubleType()),
    T.StructField("sessionId", T.LongType()),
    T.StructField("song", T.StringType()),
    T.StructField("status", T.LongType()),
    T.StructField("ts", T.LongType()),
    T.StructField("userAgent", T.StringType()),
    T.StructField("userId", T.StringType()),
    # Lines that don't match the schema land here instead of failing the job.
    T.StructField("_corrupt_record", T.StringType()),
])


# ---------------------------------------------------------- spark setup

def _bundled_hadoop_version() -> str:
    """Hadoop version shipped inside the installed pyspark (hadoop-aws must match it)."""
    import pyspark

    jars = glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "hadoop-client-api-*.jar"))
    if not jars:
        raise RuntimeError("Could not find hadoop-client-api jar inside pyspark")
    return os.path.basename(jars[0])[len("hadoop-client-api-"):-len(".jar")]


def build_spark(local_s3: bool, small_data: bool) -> SparkSession:
    builder = (
        SparkSession.builder.appName("sparkify-data-lake")
        # Overwrite only the partitions present in this run's output, not the whole table.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")  # keep logs readable
    )
    if small_data:
        # The default of 200 shuffle partitions is tuned for clusters; on a laptop
        # with a few MB of data it just creates hundreds of tiny tasks.
        builder = builder.config("spark.sql.shuffle.partitions", "8")
    if local_s3:
        hadoop = _bundled_hadoop_version()
        major_minor = tuple(int(x) for x in hadoop.split(".")[:2])
        # Hadoop 3.4+ uses AWS SDK v2; older versions use SDK v1 class names.
        provider = (
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
            if major_minor >= (3, 4)
            else "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        builder = (
            builder.config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop}")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
        )
    return builder.getOrCreate()


def to_spark_path(path: str, local_s3: bool) -> str:
    # EMR understands s3:// natively (EMRFS). Open-source Spark needs s3a://.
    if local_s3 and path.startswith("s3://"):
        return "s3a://" + path[len("s3://"):]
    return path.rstrip("/")


# ---------------------------------------------------------------- extract

def read_songs(spark: SparkSession, root: str) -> DataFrame:
    return (
        spark.read.schema(SONG_SCHEMA)
        .option("recursiveFileLookup", "true")  # song files are nested 3 folders deep
        .json(f"{root}/song_data")
    )


def read_logs(spark: SparkSession, root: str) -> DataFrame:
    return (
        spark.read.schema(LOG_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("recursiveFileLookup", "true")
        .json(f"{root}/log_data")
        .cache()  # read once, used for both good and quarantined rows
    )


# -------------------------------------------------------------- transform

def songs_table(songs_raw: DataFrame) -> DataFrame:
    return (
        songs_raw.select(
            "song_id",
            F.trim("title").alias("title"),
            "artist_id",
            F.when(F.col("year") > 0, F.col("year")).alias("year"),  # 0 means unknown
            "duration",
        )
        .where(F.col("song_id").isNotNull())
        .dropDuplicates(["song_id"])
    )


def artists_table(songs_raw: DataFrame) -> DataFrame:
    trimmed = F.trim("artist_location")
    location = F.when(trimmed != "", trimmed)  # empty string -> NULL
    df = songs_raw.select(
        "artist_id",
        F.trim("artist_name").alias("name"),
        location.alias("location"),
        F.col("artist_latitude").alias("latitude"),
        F.col("artist_longitude").alias("longitude"),
    )
    # Keep one row per artist, preferring one that has a location.
    w = Window.partitionBy("artist_id").orderBy(F.col("location").isNull(), F.col("name"))
    return df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")


def song_plays(logs: DataFrame) -> DataFrame:
    """NextSong events by logged-in users, de-duplicated, with a real timestamp."""
    return (
        logs.where(F.col("_corrupt_record").isNull())
        .where((F.col("page") == "NextSong") & (F.trim(F.coalesce("userId", F.lit(""))) != ""))
        .dropDuplicates(["userId", "sessionId", "itemInSession", "ts"])
        .withColumn("user_id", F.col("userId").cast("int"))
        .withColumn("start_time", (F.col("ts") / 1000).cast("timestamp"))
    )


def users_table(plays: DataFrame) -> DataFrame:
    # Latest attributes per user (level changes when someone upgrades).
    w = Window.partitionBy("user_id").orderBy(F.col("ts").desc())
    return (
        plays.withColumn("rn", F.row_number().over(w))
        .where("rn = 1")
        .select(
            "user_id",
            F.col("firstName").alias("first_name"),
            F.col("lastName").alias("last_name"),
            "gender",
            "level",
        )
    )


def time_table(plays: DataFrame) -> DataFrame:
    t = plays.select("start_time").distinct()
    return t.select(
        "start_time",
        F.hour("start_time").alias("hour"),
        F.dayofmonth("start_time").alias("day"),
        F.weekofyear("start_time").alias("week"),
        F.month("start_time").alias("month"),
        F.year("start_time").alias("year"),
        # Spark's dayofweek is 1 = Sunday; shift so 0 = Monday like the other projects.
        ((F.dayofweek("start_time") + 5) % 7).alias("weekday"),
    )


def songplays_table(plays: DataFrame, songs: DataFrame, artists: DataFrame) -> DataFrame:
    catalog = (
        songs.join(artists.select("artist_id", F.col("name").alias("artist_name")), "artist_id")
        .select(
            "song_id",
            "artist_id",
            F.lower("title").alias("title_key"),
            F.lower("artist_name").alias("artist_key"),
            F.round("duration", 2).alias("duration_key"),
        )
        .dropDuplicates(["title_key", "artist_key", "duration_key"])
    )
    p = plays.select(
        "*",
        F.lower("song").alias("title_key"),
        F.lower("artist").alias("artist_key"),
        F.round("length", 2).alias("duration_key"),
    )
    joined = p.join(F.broadcast(catalog), ["title_key", "artist_key", "duration_key"], "left")
    return joined.select(
        # A deterministic id (unlike monotonically_increasing_id) stays stable across re-runs.
        F.sha2(F.concat_ws("|", "user_id", "sessionId", "ts"), 256).alias("songplay_id"),
        "start_time",
        "user_id",
        "level",
        "song_id",
        "artist_id",
        F.col("sessionId").alias("session_id"),
        "location",
        F.col("userAgent").alias("user_agent"),
        F.year("start_time").alias("year"),
        F.month("start_time").alias("month"),
    )


# ------------------------------------------------------------------- load

def write(df: DataFrame, path: str, partition_by: list | None = None, files: int | None = None) -> None:
    """Write Parquet; control file counts so we don't litter the lake with tiny files."""
    if partition_by:
        df = df.repartition(*partition_by)  # one task (≈ one file) per partition value
    elif files:
        df = df.coalesce(files)
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


# ---------------------------------------------------------------- checks

def check(name: str, df: DataFrame, key: list) -> int:
    total = df.count()
    distinct = df.select(*key).distinct().count()
    null_keys = df.where(" OR ".join(f"{k} IS NULL" for k in key)).count()
    problems = []
    if total == 0:
        problems.append("is empty")
    if distinct != total:
        problems.append(f"has {total - distinct} duplicate keys")
    if null_keys:
        problems.append(f"has {null_keys} NULL keys")
    status = "FAIL " + ", ".join(problems) if problems else "PASS"
    print(f"  {name:<10} {total:>7} rows  [{status}]")
    if problems:
        raise ValueError(f"Data quality check failed for {name}: {problems}")
    return total


def main(argv: list | None = None) -> None:
    p = argparse.ArgumentParser(description="Sparkify data lake ETL")
    p.add_argument("--input", required=True, help="folder/URI containing song_data/ and log_data/")
    p.add_argument("--output", required=True, help="folder/URI for the curated Parquet tables")
    p.add_argument("--local-s3", action="store_true", help="enable s3a:// access when running outside EMR")
    args = p.parse_args(argv)

    src = to_spark_path(args.input, args.local_s3)
    out = to_spark_path(args.output, args.local_s3)
    on_local_disk = "://" not in src
    spark = build_spark(args.local_s3, small_data=on_local_disk)
    spark.sparkContext.setLogLevel("WARN")

    songs_raw = read_songs(spark, src)
    logs = read_logs(spark, src)

    bad = logs.where(F.col("_corrupt_record").isNotNull())
    n_bad = bad.count()
    if n_bad:
        print(f"Quarantining {n_bad} malformed log lines -> {out}/_quarantine/log_data")
        bad.select("_corrupt_record").write.mode("overwrite").text(f"{out}/_quarantine/log_data")

    songs = songs_table(songs_raw).cache()
    artists = artists_table(songs_raw).cache()
    plays = song_plays(logs).cache()
    users = users_table(plays)
    time_df = time_table(plays)
    songplays = songplays_table(plays, songs, artists).cache()

    print("Data quality checks:")
    check("songs", songs, ["song_id"])
    check("artists", artists, ["artist_id"])
    check("users", users, ["user_id"])
    check("time", time_df, ["start_time"])
    n_plays = check("songplays", songplays, ["songplay_id"])
    matched = songplays.where(F.col("song_id").isNotNull()).count()
    print(f"  {matched}/{n_plays} plays matched a catalog song")

    print(f"Writing Parquet to {out}")
    write(songs, f"{out}/songs", partition_by=["year"])
    write(artists, f"{out}/artists", files=1)
    write(users, f"{out}/users", files=1)
    write(time_df, f"{out}/time", partition_by=["year", "month"])
    write(songplays, f"{out}/songplays", partition_by=["year", "month"])

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main(sys.argv[1:])

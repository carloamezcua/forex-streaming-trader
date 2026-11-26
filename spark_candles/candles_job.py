from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


TICK_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


def parse_flexible_timestamp(col_name: str = "timestamp"):
    """Parse ISO-8601 UTC strings, Unix seconds, or Unix milliseconds into Spark TimestampType."""
    raw = F.trim(F.col(col_name))
    is_numeric = raw.rlike(r"^\d+(\.\d+)?$")
    numeric_val = F.when(is_numeric, raw.cast("double"))

    from_epoch = F.when(
        numeric_val > F.lit(1e12),
        (numeric_val / F.lit(1000.0)).cast("timestamp"),
    ).otherwise(numeric_val.cast("timestamp"))

    from_iso = F.coalesce(
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(raw),
    )
    return F.when(is_numeric, from_epoch).otherwise(from_iso)


def build_candles_dataframe(raw_kafka_df: DataFrame, watermark_delay: str = "10 seconds") -> DataFrame:
    """Transform raw Kafka tick rows into 1-minute OHLC candle rows formatted for Kafka sink."""
    parsed = (
        raw_kafka_df.select(
            F.col("offset").alias("kafka_offset"),
            F.from_json(F.col("value").cast("string"), TICK_SCHEMA).alias("data"),
        )
        .select("kafka_offset", "data.*")
        .withColumn(
            "symbol",
            F.regexp_replace(F.upper(F.trim(F.col("symbol"))), r"[/.]", ""),
        )
        .withColumn("event_time", parse_flexible_timestamp("timestamp"))
        .filter(
            F.col("symbol").rlike(r"^[A-Z]{6}$")
            & F.col("bid").isNotNull()
            & F.col("ask").isNotNull()
            & (F.col("bid") > 0)
            & (F.col("ask") >= F.col("bid"))
            & F.col("event_time").isNotNull()
        )
        .withColumn("mid", (F.col("bid") + F.col("ask")) / F.lit(2.0))
    )

    candles = (
        parsed.withWatermark("event_time", watermark_delay)
        .groupBy(
            F.window(F.col("event_time"), "1 minute").alias("w"),
            F.col("symbol"),
        )
        .agg(
            F.min(F.struct(F.col("event_time"), F.col("kafka_offset"), F.col("mid")))
            .getField("mid")
            .alias("open"),
            F.max("mid").alias("high"),
            F.min("mid").alias("low"),
            F.max(F.struct(F.col("event_time"), F.col("kafka_offset"), F.col("mid")))
            .getField("mid")
            .alias("close"),
            F.count("*").alias("num_ticks"),
        )
        .select(
            F.col("symbol"),
            F.date_format(F.col("w.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("inicio"),
            F.round(F.col("open"), 6).alias("open"),
            F.round(F.col("high"), 6).alias("high"),
            F.round(F.col("low"), 6).alias("low"),
            F.round(F.col("close"), 6).alias("close"),
            F.col("num_ticks").cast("long").alias("num_ticks"),
        )
    )

    return candles.select(
        F.col("symbol").alias("key"),
        F.to_json(
            F.struct("symbol", "inicio", "open", "high", "low", "close", "num_ticks")
        ).alias("value"),
    )


def main() -> None:
    """Run the Spark Structured Streaming job transforming 'ticks' into 1-minute 'velas'."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic_ticks = os.getenv("TOPIC_TICKS", "ticks")
    topic_velas = os.getenv("TOPIC_VELAS", "velas")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints/velas_1m")
    watermark_delay = os.getenv("SPARK_WATERMARK_DELAY", "10 seconds")
    starting_offsets = os.getenv("SPARK_STARTING_OFFSETS", "latest")

    spark = (
        SparkSession.builder.appName("ForexTicksToCandles1m")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic_ticks)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    output_df = build_candles_dataframe(raw_stream, watermark_delay=watermark_delay)

    query = (
        output_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic_velas)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

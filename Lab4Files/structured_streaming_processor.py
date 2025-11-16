#!/usr/bin/env python3
"""
Lab 4 Part 2: Structured Streaming Processor
Real-time IoT sensor data processing using Spark Structured Streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, count,
    current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, TimestampType
)

import os
import shutil

def create_spark_session():
    """Create Spark session with Kafka support"""

    checkpoint_dir = "/tmp/spark-checkpoint"
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
    os.makedirs(checkpoint_dir, exist_ok=True)

    return SparkSession.builder \
        .appName("IoT Structured Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .getOrCreate()

def define_sensor_schema():
    """Define schema for IoT sensor data"""
    return StructType([
        StructField("sensor_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", StringType(), True),
        StructField("battery_level", DoubleType(), True),
        StructField("signal_strength", DoubleType(), True)
    ])

def process_stream():
    """Main streaming processing function"""
    
    print("=" * 60)
    print("Starting Structured Streaming Processor")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Define schema
    sensor_schema = define_sensor_schema()
    
    # Read from Kafka
    print("\n[1] Reading stream from Kafka topic: iot-sensors")
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.25.0.13:9092") \
        .option("subscribe", "iot-sensors") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    print("[2] Parsing JSON messages")
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), sensor_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    # Add watermark for late data handling (2 minutes)
    watermarked_df = parsed_df \
        .withWatermark("timestamp", "2 minutes")
    
    # Aggregation 1: 30-second tumbling window statistics per sensor type
    print("[3] Computing 30-second window aggregations")
    window_stats = watermarked_df \
        .groupBy(
            window(col("timestamp"), "30 seconds"),
            col("sensor_type")
        ) \
        .agg(
            count("*").alias("message_count"),
            avg("temperature").alias("avg_temperature"),
            max("temperature").alias("max_temperature"),
            min("temperature").alias("min_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("battery_level").alias("avg_battery"),
            avg("signal_strength").alias("avg_signal")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "sensor_type",
            "message_count",
            "avg_temperature",
            "max_temperature",
            "min_temperature",
            "avg_humidity",
            "avg_battery",
            "avg_signal"
        )
    
    # Detect anomalies (high temperature alerts)
    print("[4] Setting up anomaly detection")
    alerts_df = watermarked_df \
        .filter(
            (col("temperature") > 35.0) | 
            (col("battery_level") < 20.0)
        ) \
        .select(
            col("timestamp"),
            col("sensor_id"),
            col("sensor_type"),
            col("temperature"),
            col("battery_level"),
            col("location"),
            expr("CASE WHEN temperature > 35.0 THEN 'HIGH_TEMP' " +
                 "WHEN battery_level < 20.0 THEN 'LOW_BATTERY' " +
                 "ELSE 'UNKNOWN' END").alias("alert_type")
        )
    
    # Query 1: Console output for window statistics
    print("\n[5] Starting output queries...")
    query1 = window_stats \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .queryName("WindowStatistics") \
        .start()
    
    # Query 2: Console output for alerts
    query2 = alerts_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .queryName("Alerts") \
        .start()

    
    # Query 3: Memory sink for real-time monitoring (optional)
    query3 = watermarked_df \
        .writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName("live_sensors") \
        .start()    

    print("\n" + "=" * 60)
    print("Streaming Processor Running")
    print("=" * 60)
    print("\nMonitoring:")
    print("  - Spark UI: http://localhost:4040")
    print("  - Watch console for statistics and alerts")
    print("\nPress Ctrl+C to stop")
    print("=" * 60 + "\n")
    
    # Keep queries running
    try:
        query1.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nStopping streaming queries...")
        query1.stop()
        query2.stop()
        # query3.stop()
        spark.stop()
        print("Shutdown complete.")

if __name__ == "__main__":
    try:
        process_stream()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

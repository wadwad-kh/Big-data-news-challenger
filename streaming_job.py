import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, explode, split, lower, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def main():
    # 1. Create SparkSession
    spark = SparkSession.builder \
        .appName("NewsPulseStreaming") \
        .master("local[*]") \
        .getOrCreate()

    # Suppress excessive logging
    spark.sparkContext.setLogLevel("ERROR")

    # Ensure output directory exists for Streamlit communication
    os.makedirs("data/output", exist_ok=True)

    # 2. Define Schema
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("url", StringType(), True),
        StructField("ts", TimestampType(), True)
    ])

    # 3. Read Stream
    df = spark.readStream \
        .schema(schema) \
        .json("data/incoming")

    # 4. Aggregations & Memory Tables

    # Query 1: Count headlines per source
    by_source_df = df.groupBy("source").count()
    
    query_by_source = by_source_df.writeStream \
        .format("memory") \
        .outputMode("complete") \
        .queryName("by_source") \
        .start()

    # Query 2: Count headlines per 1-hour window
    by_window_df = df.withWatermark("ts", "1 hour") \
        .groupBy(window(col("ts"), "1 hour")) \
        .count()
    
    query_by_window = by_window_df.writeStream \
        .format("memory") \
        .outputMode("complete") \
        .queryName("by_window") \
        .start()

    # Query 3: Top 10 keywords after stop-word filtering
    stop_words = ["the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "with", "is", "are", "was", "were", "of", "it", "as", "by", "that", "this"]
    
    # Clean and extract words
    words_df = df.select(
        explode(
            split(lower(regexp_replace(col("title"), "[^a-zA-Z\\s]", "")), "\\s+")
        ).alias("word")
    )
    
    top_words_df = words_df \
        .filter(col("word") != "") \
        .filter(~col("word").isin(stop_words)) \
        .groupBy("word") \
        .count() \
        .orderBy(col("count").desc()) \
        .limit(10)
    
    query_top_words = top_words_df.writeStream \
        .format("memory") \
        .outputMode("complete") \
        .queryName("top_words") \
        .start()

    print("Spark Streaming Job Started. Pushing data to Streamlit every 5 seconds...")

    # 5. Background Loop: Dump memory tables to CSV so Streamlit can read them
    try:
        while True:
            # Check if any queries failed
            if any(q.exception() for q in [query_by_source, query_by_window, query_top_words]):
                print("A query failed. Exiting.")
                break

            # Dump memory tables to CSVs using toPandas()
            try:
                # Get data from memory tables
                source_pd = spark.sql("SELECT * FROM by_source").toPandas()
                window_pd = spark.sql("SELECT window.start as window_start, window.end as window_end, count FROM by_window ORDER BY window_start").toPandas()
                top_words_pd = spark.sql("SELECT * FROM top_words").toPandas()

                # Save to CSV files
                source_pd.to_csv("data/output/by_source.csv", index=False)
                window_pd.to_csv("data/output/by_window.csv", index=False)
                top_words_pd.to_csv("data/output/top_words.csv", index=False)
                
            except Exception as e:
                # Table might not be fully initialized yet
                pass

            time.sleep(5)
    except KeyboardInterrupt:
        print("Stopping streaming job...")
    finally:
        query_by_source.stop()
        query_by_window.stop()
        query_top_words.stop()
        spark.stop()

if __name__ == "__main__":
    main()

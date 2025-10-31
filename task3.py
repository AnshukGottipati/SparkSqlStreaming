from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum as sparksum,to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
df = (
    spark.readStream
         .format("socket")
         .option("host", "localhost")
         .option("port", 9999)
         .load()
)
# Parse JSON data into columns using the defined schema
parse_df = (
    df
    .select(from_json(col("value"), schema).alias("json"))
    .select(
        col("json.trip_id"),
        col("json.driver_id"),
        col("json.distance_km"),
        col("json.fare_amount"),
        to_timestamp(col("json.timestamp")).alias("timestamp")
    )
)
# Convert timestamp column to TimestampType and add a watermark
watermarked_df = parse_df.withWatermark("timestamp", "1 minute")
# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_agg = (
    watermarked_df
    .groupBy(
        col("driver_id"),
        window(col("timestamp"), "5 minutes", "1 minute")
    )
    .agg(sparksum("fare_amount").alias("window_total_fare"))
)

# Extract window start and end times as separate columns
result_df = (
    windowed_agg
    .select(
        col("driver_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("window_total_fare")
    )
)
# Define a function to write each batch to a CSV file
def write_batch(batch_df, batch_id):
    # Save the batch DataFrame as a CSV file with the batch ID in the filename
    (batch_df
        .coalesce(1) 
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(f"./outputs/task_3/batch_id={batch_id}")
    )
    



# Use foreachBatch to apply the function to each micro-batch
query = (
    result_df.writeStream
    .outputMode("update")
    .foreachBatch(write_batch)
    .start()
)
query.awaitTermination()

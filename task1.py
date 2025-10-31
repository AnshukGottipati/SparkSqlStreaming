from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
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
# Print parsed data to the CSV files
query = (
    parse_df.writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode("append")
    .start()
)

query.awaitTermination()

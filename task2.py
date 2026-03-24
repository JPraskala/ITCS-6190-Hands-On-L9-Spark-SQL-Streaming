from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("Real-Time Aggregations").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
init_df = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Parse JSON data into columns using the defined schema
parsed_df = init_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
parsed_df = parsed_df.withWatermark("timestamp", "30 seconds")

# Compute aggregations: total fare and average distance grouped by driver_id
result_df = parsed_df.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Define a function to write each batch to a CSV file

    # Save the batch DataFrame as a CSV file with the batch ID in the filename

def write_to_csv(batch_df, batch_id):
    batch_df.write.csv(f"outputs/task_2/batch_{batch_id}", header=True, mode="overwrite")
    


# Use foreachBatch to apply the function to each micro-batch
query = result_df.writeStream.outputMode("complete").foreachBatch(write_to_csv).start()


query.awaitTermination()

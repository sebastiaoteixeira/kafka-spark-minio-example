from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session with additional config to disable metrics warnings
spark = SparkSession.builder \
    .appName("KafkaToLocalStorage") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.cores", "1") \
    .config("spark.metrics.conf.*.sink.console.period", "0") \
    .config("spark.metrics.conf.*.sink.console.unit", "seconds") \
    .getOrCreate()

# Kafka broker and topic details
kafka_bootstrap_servers = "kafka:19092"
kafka_topic = "topic1"

# Define the schema for the incoming Kafka messages (assuming JSON format)
json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

# Read streaming data from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka 'value' column from binary to string and apply the schema
df = raw_df.select(from_json(col("value").cast("string"), json_schema).alias("data"))

# You can perform any transformations on the 'data' here

# Write processed data to local storage as Parquet format
output_path = "/tmp/spark_output"
df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .option("path", output_path) \
    .start()

# Wait for termination of the stream
spark.streams.awaitAnyTermination()

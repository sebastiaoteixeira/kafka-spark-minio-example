from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session with MinIO configurations
spark = SparkSession.builder \
    .appName("KafkaToMinio") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.cores", "1") \
    .config("spark.metrics.conf.*.sink.console.period", "0") \
    .config("spark.metrics.conf.*.sink.console.unit", "seconds") \
    .config("spark.hadoop.fs.s3a.access.key", "user") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.fileSink.log.compactInterval", "100")

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
    .option("failOnDataLoss", "false") \
    .load()

# Convert Kafka 'value' column from binary to string and apply the schema
df = raw_df.select(from_json(col("value").cast("string"), json_schema).alias("data"))

# Flatten the JSON structure to create a tabular format
tabular_df = df.select(
    col("data.id").alias("id"),
    col("data.name").alias("name"),
    col("data.email").alias("email")
)

# Write the processed data to MinIO as Parquet format
output_path = "s3a://my-bucket/kafka_data"
tabular_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://my-bucket/spark-checkpoints") \
    .option("path", output_path) \
    .start()

# Wait for termination of the stream
spark.streams.awaitAnyTermination()

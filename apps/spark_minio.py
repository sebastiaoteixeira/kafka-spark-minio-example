from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize Spark session with MinIO configurations
spark = SparkSession.builder \
    .appName("SparkToMinioExample") \
    .config("spark.hadoop.fs.s3a.access.key", "user") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Example data to be written
data = [
    Row(id=1, name="John Doe", email="john.doe@example.com"),
    Row(id=2, name="Jane Smith", email="jane.smith@example.com"),
    Row(id=3, name="Samuel Johnson", email="samuel.johnson@example.com")
]

# Create DataFrame
df = spark.createDataFrame(data)

# Write the DataFrame to MinIO (using s3a://) in Parquet format
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "s3a://my-bucket/test_data") \
    .save()

print("Data successfully written to MinIO!")

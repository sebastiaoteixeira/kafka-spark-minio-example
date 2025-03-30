# Kafka Spark MinIO App Example

This is an Apache Spark app example integrated with Kafka and MinIO. The app reads messages from a Kafka topic, processes them and writes the results to MinIO.


## Setup

To use the app, you need to have a Kafka broker and a MinIO server running. You can use the `docker-compose.yml` file in the root directory to start both services.

```bash
docker compose up
```

You should create a bucket and a user on MinIO.

You can use the MinIO UI in localhost:9001

## Running the app

To run the app, use the following command:

```bash
docker exec -it spark-spark-master-1 /opt/spark/bin/spark-submit   --master spark://spark-master:7077   --deploy-mode client   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2,org.apache.hadoop:hadoop-aws:3.2.0   --py-files /opt/spark-apps/spark_app.py   /opt/spark-apps/spark_app.py
```

There are 2 another available .py apps:
- spark_kafke: reads messages from a Kafka topic and writes them to ./tmp directory
- spark_minio: writes static data to MinIO

To produce random messages to kafka, you can run the producer.py script:

```bash
python producer.py
```

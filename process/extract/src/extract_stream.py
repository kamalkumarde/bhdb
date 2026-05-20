# spark/src/spark_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
import logging

# Set up logging to standard output
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("ExtractionLogger")

print("🚀 Starting Spark Structured Streaming job to extract data from Kafka and write to S3...")
spark = SparkSession.builder \
    .appName("KubernetesKafkaToS3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-service.bhdb-data-platform.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "supersecret") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
print("✅ Spark session initialized with S3 configurations.")

schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", IntegerType(), True)
])

print("🎯 Subscribing to Kafka topic 'transactions' for real-time data ingestion...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-service.bhdb-data-platform.svc.cluster.local:9092") \
    .option("subscribe", "transactions") \
    .load()
print("✅ Connected to Kafka and streaming data from 'transactions' topic.")

parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")).select("data.*")

print("📋 Target Stream Schema:")
parsed_df.printSchema() # This executes standalone safely

# Custom function to log every transaction read and then write to S3
def log_and_save_batch(batch_df, batch_id):
    record_count = batch_df.count()
    if record_count > 0:
        logger.info(f"📥 Processing batch {batch_id} containing {record_count} transactions:")
        rows = batch_df.collect()
        for row in rows:
            logger.info(f"👉 TX_READ: ID={row['event_id']} | User={row['user_id']} | Amt=${row['amount']} | TS={row['timestamp']}")
            
        # Write batch to S3
        batch_df.write \
            .format("parquet") \
            .mode("append") \
            .save("s3a://analytics-bucket/raw_transactions/")
    else:
        logger.debug(f"💤 Batch {batch_id} is empty.")

print("🚀 Starting the unified Logging & S3 Storage stream...")
query = parsed_df.writeStream \
    .foreachBatch(log_and_save_batch) \
    .option("checkpointLocation", "s3a://analytics-bucket/checkpoints/") \
    .start()

query.awaitTermination()
print("🛑 Streaming job has been terminated.")

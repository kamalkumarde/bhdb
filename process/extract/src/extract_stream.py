# spark/src/spark_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("IcebergExtractionLogger")

print("🚀 Starting Spark Structured Streaming job with Iceberg support...")

# Initialize Spark with Corrected MinIO S3 and Iceberg Configurations
spark = SparkSession.builder \
    .appName("KubernetesKafkaToIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "hadoop") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://analytics-bucket/warehouse") \
    # FIX: Pointed directly back to your MinIO service instance container port
    .config("spark.hadoop.fs.s3a.endpoint", "http://cluster.local") \
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
print("✅ Defined schema for incoming Kafka JSON data.")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-service.bhdb-data-platform.svc.cluster.local:9092") \
    .option("subscribe", "transactions") \
    .load()
print("✅ Connected to Kafka topic 'transactions' and started streaming data.")

parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")).select("data.*")
print("✅ Parsed incoming Kafka JSON data into structured format with defined schema.")

# Create the Iceberg table if it does not already exist in the catalog
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.db.raw_transactions (
        event_id INT,
        user_id INT,
        amount DOUBLE,
        timestamp INT
    ) USING iceberg
""")
print("✅ Iceberg table 'demo.db.raw_transactions' is verified/created.")

# Stream writing using the modern Iceberg sink layout
query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://analytics-bucket/checkpoints/iceberg_raw/") \
    .toTable("demo.db.raw_transactions")

print("🚀 Streaming data is spinning into Iceberg table storage format continuously...")

# Block main thread execution to let background threads ingest data safely
query.awaitTermination()

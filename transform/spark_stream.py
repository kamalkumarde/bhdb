# app/spark_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaToS3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://local-s3:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "supersecret") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", IntegerType(), True)
])

# Read from Local Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "local-kafka:9092") \
    .option("subscribe", "transactions") \
    .load()

# Parse JSON payload
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Write to Local S3 (MinIO)
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://analytics-bucket/raw_transactions/") \
    .option("checkpointLocation", "s3a://analytics-bucket/checkpoints/") \
    .start()

query.awaitTermination()
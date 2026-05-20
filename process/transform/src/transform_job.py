# process/transform/src/transform_job.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when

# Initialize optimized Spark context for S3 storage tracking
spark = SparkSession.builder \
    .appName("S3-Bronze-To-Silver-Transform") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-service.bhdb-data-platform.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "supersecret") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("🧹 Transform Engine started. Processing Raw Bronze Layer data...")

# Read the raw transactional stream directly from your Extract landing zone
raw_df = spark.readStream \
    .format("parquet") \
    .schema("event_id INT, user_id INT, amount DOUBLE, timestamp INT") \
    .load("s3a://analytics-bucket/raw_transactions/")

# Apply transformations, business logic adjustments, and track processing timestamps
transformed_df = raw_df \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("transaction_status", when(col("amount") > 400.0, "FLAGGED").otherwise("APPROVED")) \
    .filter(col("user_id").isNotNull())

# Write down stream straight into your Silver Layer partition directory
query = transformed_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://analytics-bucket/transformed_transactions/") \
    .option("checkpointLocation", "s3a://analytics-bucket/checkpoints/transform/") \
    .start()

query.awaitTermination()
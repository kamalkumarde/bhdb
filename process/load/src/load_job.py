# process/load/src/load_job.py
import time
import json
import boto3
import pandas as pd
from io import BytesIO

print("📥 Aggregation and Load engine initializing...")

# Point straight to your internal MinIO Object storage wrapper
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio-service.bhdb-data-platform.svc.cluster.local:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='supersecret',
    config=boto3.session.Config(signature_version='s3v4')
)

BUCKET_NAME = "analytics-bucket"
PREFIX = "transformed_transactions/"

def run_analytical_aggregation():
    try:
        # Pull tracking maps directly from our Silver layer buckets
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
        if 'Contents' not in response:
            print("No processed partition logs discovered yet. Waiting...")
            return

        # Read discovered metrics into a single localized dataframe analysis block
        data_frames = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=obj['Key'])
                df = pd.read_parquet(BytesIO(file_obj['Body'].read()))
                data_frames.append(df)

        if not data_frames:
            return

        combined_df = pd.concat(data_frames, ignore_index=True)
        
        # Calculate real-time high-level tracking aggregates
        summary_metrics = {
            "total_transactions_processed": int(len(combined_df)),
            "total_volume_usd": float(combined_df['amount'].sum()),
            "average_ticket_size": float(combined_df['amount'].mean()),
            "flagged_events_count": int((combined_df['transaction_status'] == 'FLAGGED').sum()),
            "last_updated_epoch": int(time.time())
        }

        # Save metric outcomes directly into our Gold Analytical summary payload
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key="gold_metrics/summary.json",
            Body=json.dumps(summary_metrics, indent=4).encode('utf-8')
        )
        print(f"📊 Gold analytics tracking node updated: {summary_metrics}")

    except Exception as e:
        print(f"❌ Error encountered processing Load layer: {str(e)}")

# Continuous processing execution loop
while True:
    run_analytical_aggregation()
    time.sleep(10)
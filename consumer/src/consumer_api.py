# consumer/src/consumer_api.py
import json
import boto3
from fastapi import FastAPI, HTTPException
import uvicorn

app = FastAPI(title="BHDB Data Platform Analytics Portal")

# Internal connection configurations pointing directly back to MinIO storage layer
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio-service.bhdb-data-platform.svc.cluster.local:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='supersecret'
)

@app.get("/health")
def health_check():
    return {"status": "ONLINE", "service": "consumer-api"}

@app.get("/api/v1/metrics/summary")
def get_platform_metrics():
    try:
        # Fetch down current computed data payload tracking files
        file_obj = s3_client.get_object(Bucket="analytics-bucket", Key="gold_metrics/summary.json")
        metrics_data = json.loads(file_obj['Body'].read().decode('utf-8'))
        return {
            "status": "SUCCESS",
            "data": metrics_data
        }
    except s3_client.exceptions.NoSuchKey:
        raise HTTPException(
            status_code=404, 
            detail="Analytics metrics haven't been computed by the Load layer yet."
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal API Error: {str(e)}")

if __name__ == "__main__":
    # Start the application server on cluster standard networking container port
    uvicorn.run(app, host="0.0.0.0", port=8000)
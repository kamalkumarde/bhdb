#!/bin/bash


echo "🧹 Cleaning up existing deployments in namespace '$NAMESPACE'..."
set -e
echo "🐳 Pointing shell to Minikube's Docker daemon..."
eval $(minikube docker-env)
NAMESPACE="bhdb-data-platform"
echo "🧹 Cleaning up existing deployments..."
# Instead of: kubectl delete deployments --all -n $NAMESPACE
# Use this specific list:
kubectl delete deployment data-producer data-extract-engine data-transform-engine data-load-engine platform-consumer -n $NAMESPACE --ignore-not-found

echo "📦 Building new container images..."
docker build --no-cache -f producer/Dockerfile.producer -t local-producer:v1 .
docker build --no-cache  -f process/extract/Dockerfile.extract -t local-extract:v1 .
docker build --no-cache -f process/transform/Dockerfile.transform -t local-transform:v1 .
docker build --no-cache -f process/load/Dockerfile.load -t local-load:v1 .
docker build --no-cache -f consumer/Dockerfile.consumer -t local-consumer:v1 .

echo "🚀 Deploying fresh data platform workloads..."
kubectl apply -f platform-workloads.yaml -n $NAMESPACE

echo "⏱️ Waiting for rollout to complete..."
kubectl rollout status deployment/data-producer -n $NAMESPACE --timeout=60s

echo "📊 Streaming logs from data-producer (Press Ctrl+C to stop)..."
# We remove the '-c runtime' flag in case your container is named differently
kubectl logs -f deployment/data-producer -n $NAMESPACE

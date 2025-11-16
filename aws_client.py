import boto3
import os
from dotenv import load_dotenv
import botocore.config

load_dotenv()

# Optimized config for lower latency
cfg = botocore.config.Config(
    tcp_keepalive=True,
    max_pool_connections=25,
    retries={'max_attempts': 2}  # Faster retries
)

# Create a persistent session (reused across Streamlit reruns)
session = boto3.session.Session()

# Global DynamoDB client reused everywhere
dynamodb_client = session.client(
    "dynamodb",
    region_name=os.getenv("AWS_REGION", "ap-south-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    config=cfg
)

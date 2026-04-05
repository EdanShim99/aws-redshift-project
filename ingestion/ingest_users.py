import requests
import boto3
import json
from datetime import datetime

# ==========================
# Configuration
# ==========================

BUCKET_NAME = "ecommerce-lakehousev2"
S3_PREFIX = "bronze/users"
API_URL = "https://dummyjson.com/users"

# ==========================
# Helper: Recursively lowercase keys
# ==========================

def lowercase_keys(obj):
    if isinstance(obj, dict):
        return {k.lower(): lowercase_keys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [lowercase_keys(item) for item in obj]
    else:
        return obj

# ==========================
# Fetch data
# ==========================

print("Fetching users from DummyJSON...")

response = requests.get(API_URL)
response.raise_for_status()

data = response.json()
users = data.get("users", [])

if not users:
    raise ValueError("No users returned from API")

print(f"Fetched {len(users)} users")

# ==========================
# Normalize keys to lowercase
# ==========================

users_lower = [lowercase_keys(user) for user in users]

# Convert to newline-delimited JSON
ndjson = "\n".join(json.dumps(user) for user in users_lower)

# ==========================
# Build partition path
# ==========================

today = datetime.utcnow().strftime("%Y-%m-%d")
run_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

s3_key = f"{S3_PREFIX}/ingestion_date={today}/users_{run_timestamp}.json"

print(f"Uploading to s3://{BUCKET_NAME}/{s3_key}")

# ==========================
# Upload to S3
# ==========================

s3 = boto3.client("s3")

s3.put_object(
    Bucket=BUCKET_NAME,
    Key=s3_key,
    Body=ndjson,
    ContentType="application/json"
)

print("Upload successful ✅")
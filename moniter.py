import os
import json
import boto3
import requests

from pathlib import Path
from dotenv import load_dotenv

# =====================================================
# LOAD ENV
# =====================================================

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH)

# =====================================================
# AWS VARIABLES
# =====================================================

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
BUCKET_NAME = os.getenv("S3_BUCKET")

# =====================================================
# SLACK WEBHOOK
# =====================================================

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# =====================================================
# VALIDATION
# =====================================================

required_vars = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "S3_BUCKET": BUCKET_NAME,
    "SLACK_WEBHOOK_URL": SLACK_WEBHOOK_URL
}

missing_vars = [
    key for key, value in required_vars.items()
    if not value
]

if missing_vars:

    raise ValueError(
        f"Missing environment variables: {missing_vars}"
    )

# =====================================================
# CREATE S3 CLIENT
# =====================================================

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# =====================================================
# STATE FILE
# =====================================================

STATE_FILE = "s3_state.json"

# =====================================================
# LOAD PREVIOUS STATE
# =====================================================

if os.path.exists(STATE_FILE):

    with open(STATE_FILE, "r") as f:
        previous_state = json.load(f)

else:

    previous_state = {}

# =====================================================
# FETCH S3 OBJECTS
# =====================================================

response = s3_client.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix="output/parquet-data/"
)

current_state = {}
updates_detected = []

# =====================================================
# DETECT CHANGES
# =====================================================

for obj in response.get("Contents", []):

    key = obj["Key"]
    last_modified = str(obj["LastModified"])

    current_state[key] = last_modified

    if key not in previous_state:

        updates_detected.append(
            f"🆕 NEW FILE:\n{key}"
        )

    elif previous_state[key] != last_modified:

        updates_detected.append(
            f"♻ UPDATED FILE:\n{key}"
        )

# =====================================================
# SEND SLACK ALERT
# =====================================================

if updates_detected:

    slack_message = {
        "text": (
            "*S3 ETL Pipeline Alert*\n\n"
            + "\n\n".join(updates_detected)
        )
    }

    response = requests.post(
        SLACK_WEBHOOK_URL,
        json=slack_message
    )

    if response.status_code == 200:

        print("Slack notification sent successfully")

    else:

        print(
            f"Slack Error: {response.status_code}, "
            f"{response.text}"
        )

else:

    print("No changes detected")

# =====================================================
# SAVE CURRENT STATE
# =====================================================

with open(STATE_FILE, "w") as f:

    json.dump(current_state, f)

print("State file updated")
import os
import json
import boto3
import requests

from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH)

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")

AWS_SECRET_KEY = os.getenv(
    "AWS_SECRET_ACCESS_KEY"
)

AWS_REGION = os.getenv(
    "AWS_REGION",
    "ap-south-1"
)

BUCKET_NAME = os.getenv("S3_BUCKET")

SLACK_WEBHOOK_URL = os.getenv(
    "SLACK_WEBHOOK_URL"
)

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
        f"Missing environment variables: "
        f"{missing_vars}"
    )

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

STATE_KEY = "monitor/monitor_state.json"

def load_previous_state():

    try:

        response = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=STATE_KEY
        )

        return json.loads(
            response["Body"]
            .read()
            .decode("utf-8")
        )

    except:

        return {
            "last_processed_timestamp": None
        }

def save_current_state(state):

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=STATE_KEY,
        Body=json.dumps(
            state,
            indent=4
        )
    )

previous_state = load_previous_state()

last_processed_timestamp = previous_state.get(
    "last_processed_timestamp"
)

print("Fetching monitoring logs...")

response = s3_client.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix="monitor/"
)

all_objects = response.get("Contents", [])

timestamp_folders = set()

for obj in all_objects:

    key = obj["Key"]

    parts = key.split("/")

    if len(parts) >= 2:

        timestamp_folders.add(parts[1])

timestamp_folders = sorted(timestamp_folders)

latest_timestamp = None

if timestamp_folders:

    latest_timestamp = timestamp_folders[-1]

print(f"Latest timestamp found: {latest_timestamp}")

updates_detected = False

slack_logs = []

if latest_timestamp != last_processed_timestamp:

    updates_detected = True

    print("New monitoring logs detected")

    latest_prefix = f"monitor/{latest_timestamp}/"

    latest_response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=latest_prefix
    )

    latest_objects = latest_response.get(
        "Contents",
        []
    )

    for obj in latest_objects:

        key = obj["Key"]

        if not key.endswith(".json"):

            continue

        file_response = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=key
        )

        monitoring_data = json.loads(
            file_response["Body"]
            .read()
            .decode("utf-8")
        )

        table_name = monitoring_data.get(
            "table_name",
            "unknown"
        )

        run_timestamp = monitoring_data.get(
            "run_timestamp",
            "unknown"
        )

        total_rows = monitoring_data.get(
            "total_rows_after_update",
            0
        )

        new_count = monitoring_data.get(
            "new_records_count",
            0
        )

        duplicate_count = monitoring_data.get(
            "duplicate_records_count",
            0
        )

        historical_count = monitoring_data.get(
            "historical_records_count",
            0
        )

        new_records = monitoring_data.get(
            "new_records",
            []
        )

        duplicate_records = monitoring_data.get(
            "duplicate_records",
            []
        )

        historical_records = monitoring_data.get(
            "historical_records",
            []
        )

        log_message = (
            f"\n📊 TABLE: {table_name}"
            f"\n🕒 Run Time: {run_timestamp}"
            f"\n📦 Total Rows: {total_rows}"
            f"\n🆕 New Rows Inserted: {new_count}"
            f"\n♻ Duplicate Rows Skipped: "
            f"{duplicate_count}"
            f"\n🕘 Historical Rows Created: "
            f"{historical_count}"
        )

        if new_records:

            log_message += "\n\n🆕 NEW RECORDS:\n"

            for row in new_records[:5]:

                log_message += (
                    f"{json.dumps(row, default=str)}\n"
                )

        if duplicate_records:

            log_message += (
                "\n♻ DUPLICATE RECORDS:\n"
            )

            for row in duplicate_records[:5]:

                log_message += (
                    f"{json.dumps(row, default=str)}\n"
                )

        if historical_records:

            log_message += (
                "\n🕘 HISTORICAL RECORDS:\n"
            )

            for row in historical_records[:5]:

                log_message += (
                    f"{json.dumps(row, default=str)}\n"
                )

        slack_logs.append(log_message)

    slack_text = (

        "*🚨 ETL PIPELINE UPDATE DETECTED*\n\n"

        f"📁 Monitoring Timestamp Folder:\n"
        f"{latest_timestamp}\n\n"

        + "\n\n".join(slack_logs)
    )

    save_current_state({

        "last_processed_timestamp":
        latest_timestamp

    })

else:

    print("No new monitoring logs detected")

    slack_text = (
        "*✅ ETL MONITOR STATUS*\n\n"
        "No new updates detected.\n\n"

        f"Last Checked Timestamp:\n"
        f"{last_processed_timestamp}"
    )

slack_message = {
    "text": slack_text
}

response = requests.post(
    SLACK_WEBHOOK_URL,
    json=slack_message
)

if response.status_code == 200:

    print(
        "Slack notification sent successfully"
    )

else:

    print(
        f"Slack Error: "
        f"{response.status_code}"
    )

print("Monitoring completed")

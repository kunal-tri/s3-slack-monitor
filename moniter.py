import os
import re
import json
import boto3
import requests

from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent

ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH)

IST = ZoneInfo("Asia/Kolkata")

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

STATE_KEY = "monitor/state/monitor_state.json"

LAST_CHECKED_KEY = (
    "monitor/last_checked/"
    "last_checked.json"
)

PROCESS_LOG_KEY = (
    "monitor/history/"
)

TIMESTAMP_REGEX = re.compile(
    r"^\d{8}_\d{6}$"
)

def get_ist_time():

    return datetime.now(IST)

def get_ist_string():

    return get_ist_time().strftime(
        "%Y-%m-%d %H:%M:%S IST"
    )

def load_previous_state():

    try:

        response = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=STATE_KEY
        )

        state = json.loads(
            response["Body"]
            .read()
            .decode("utf-8")
        )

        return state

    except Exception:

        return {
            "latest_timestamp": None
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

def save_last_checked_log(data):

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=LAST_CHECKED_KEY,
        Body=json.dumps(
            data,
            indent=4
        )
    )

def save_processing_history(data):

    timestamp = datetime.now(
        IST
    ).strftime("%Y%m%d_%H%M%S")

    history_key = (
        f"{PROCESS_LOG_KEY}"
        f"{timestamp}.json"
    )

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=history_key,
        Body=json.dumps(
            data,
            indent=4
        )
    )

previous_state = load_previous_state()

saved_latest_timestamp = previous_state.get(
    "latest_timestamp"
)

monitor_checked_at = get_ist_string()

print(
    f"[{monitor_checked_at}] "
    f"Checking monitoring logs..."
)

response = s3_client.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix="monitor/"
)

all_objects = response.get(
    "Contents",
    []
)

timestamp_folders = set()

for obj in all_objects:

    key = obj["Key"]

    parts = key.split("/")

    # VALID:
    # monitor/20260519_180000/file.json

    if len(parts) >= 3:

        folder_name = parts[1]

        if TIMESTAMP_REGEX.match(
            folder_name
        ):

            timestamp_folders.add(
                folder_name
            )

timestamp_folders = sorted(
    timestamp_folders
)

print(
    f"Detected timestamps: "
    f"{timestamp_folders}"
)

previous_timestamp = saved_latest_timestamp

current_timestamp = None

new_timestamps = []

# FIRST RUN

if saved_latest_timestamp is None:

    if len(timestamp_folders) >= 2:

        previous_timestamp = (
            timestamp_folders[-2]
        )

        current_timestamp = (
            timestamp_folders[-1]
        )

        new_timestamps = [
            current_timestamp
        ]

    elif len(timestamp_folders) == 1:

        previous_timestamp = (
            timestamp_folders[0]
        )

        current_timestamp = (
            timestamp_folders[0]
        )

        new_timestamps = [
            current_timestamp
        ]

else:

    new_timestamps = [

        ts for ts in timestamp_folders

        if ts > saved_latest_timestamp
    ]

    if new_timestamps:

        current_timestamp = (
            sorted(new_timestamps)[-1]
        )

updates_detected = len(new_timestamps) > 0

slack_logs = []

processed_rows = []

if updates_detected:

    print(
        f"Processing timestamps: "
        f"{new_timestamps}"
    )

    for timestamp_folder in new_timestamps:

        latest_prefix = (
            f"monitor/"
            f"{timestamp_folder}/"
        )

        latest_response = (
            s3_client.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=latest_prefix
            )
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

                f"\n🕒 ETL Run Time: "
                f"{run_timestamp}"

                f"\n📁 Timestamp Folder: "
                f"{timestamp_folder}"

                f"\n📦 Total Rows: "
                f"{total_rows}"

                f"\n🆕 New Rows Inserted: "
                f"{new_count}"

                f"\n♻ Duplicate Rows Skipped: "
                f"{duplicate_count}"

                f"\n🕘 Historical Rows Created: "
                f"{historical_count}"
            )

            if new_records:

                log_message += (
                    "\n\n🆕 NEW RECORDS:\n"
                )

                for row in new_records[:5]:

                    processed_rows.append(row)

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

                    processed_rows.append(row)

                    log_message += (
                        f"{json.dumps(row, default=str)}\n"
                    )

            slack_logs.append(log_message)

    save_current_state({

        "latest_timestamp":
        current_timestamp

    })

    slack_text = (

        "*🚨 ETL PIPELINE UPDATE DETECTED*\n\n"

        f"🕒 Monitor Checked At:\n"
        f"{monitor_checked_at}\n\n"

        f"📌 Previous Timestamp:\n"
        f"{previous_timestamp}\n\n"

        f"📌 Latest Timestamp:\n"
        f"{current_timestamp}\n\n"

        + "\n\n".join(slack_logs)
    )

else:

    current_timestamp = previous_timestamp

    slack_text = (

        "*✅ ETL MONITOR STATUS*\n\n"

        "No new updates detected.\n\n"

        f"🕒 Monitor Checked At:\n"
        f"{monitor_checked_at}\n\n"

        f"📌 Latest Timestamp:\n"
        f"{current_timestamp}"
    )

last_checked_data = {

    "monitor_checked_at":
    monitor_checked_at,

    "previous_timestamp":
    previous_timestamp,

    "latest_timestamp":
    current_timestamp,

    "updates_detected":
    updates_detected
}

save_last_checked_log(
    last_checked_data
)

save_processing_history({

    "checked_at":
    monitor_checked_at,

    "previous_timestamp":
    previous_timestamp,

    "latest_timestamp":
    current_timestamp,

    "updates_detected":
    updates_detected,

    "processed_rows":
    processed_rows
})

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

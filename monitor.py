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

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
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

        return json.loads(
            response["Body"]
            .read()
            .decode("utf-8")
        )

    except:

        return {

            "latest_update_timestamp": None,

            "last_monitor_checked_at": None
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

previous_update_timestamp = previous_state.get(
    "latest_update_timestamp"
)

last_monitor_checked_at = previous_state.get(
    "last_monitor_checked_at"
)

current_monitor_checked_at = get_ist_string()

print(
    f"[{current_monitor_checked_at}] "
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

latest_update_timestamp = None

if timestamp_folders:

    latest_update_timestamp = (
        timestamp_folders[-1]
    )

# ONLY PROCESS NEW FOLDERS

if previous_update_timestamp is None:

    new_timestamps = timestamp_folders

else:

    new_timestamps = [

        ts for ts in timestamp_folders

        if ts > previous_update_timestamp
    ]

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

                f"\nTABLE: {table_name}"

                f"\nETL Run Time: "
                f"{run_timestamp}"

                f"\nUpdate Timestamp Folder: "
                f"{timestamp_folder}"

                f"\nTotal Rows: "
                f"{total_rows}"

                f"\nNew Rows Inserted: "
                f"{new_count}"

                f"\nDuplicate Rows Skipped: "
                f"{duplicate_count}"

                f"\nHistorical Rows Created: "
                f"{historical_count}"
            )

            if new_records:

                log_message += (
                    "\n\nNEW RECORDS:\n"
                )

                for row in new_records[:5]:

                    processed_rows.append(row)

                    log_message += (
                        f"{json.dumps(row, default=str)}\n"
                    )

            if duplicate_records:

                log_message += (
                    "\nDUPLICATE RECORDS:\n"
                )

                for row in duplicate_records[:5]:

                    log_message += (
                        f"{json.dumps(row, default=str)}\n"
                    )

            if historical_records:

                log_message += (
                    "\nHISTORICAL RECORDS:\n"
                )

                for row in historical_records[:5]:

                    processed_rows.append(row)

                    log_message += (
                        f"{json.dumps(row, default=str)}\n"
                    )

            slack_logs.append(log_message)

# ALWAYS SAVE STATE

save_current_state({

    "latest_update_timestamp":
    latest_update_timestamp,

    "last_monitor_checked_at":
    current_monitor_checked_at
})

if updates_detected:

    slack_text = (

        "*ETL PIPELINE UPDATE DETECTED*\n\n"

        f"Current Monitor Checked At:\n"
        f"{current_monitor_checked_at}\n\n"

        f"Last Monitor Checked At:\n"
        f"{last_monitor_checked_at}\n\n"

        f"Previous Update Timestamp:\n"
        f"{previous_update_timestamp}\n\n"

        f"Latest Update Timestamp:\n"
        f"{latest_update_timestamp}\n\n"

        + "\n\n".join(slack_logs)
    )

else:

    slack_text = (

        "*ETL MONITOR STATUS*\n\n"

        "No new updates detected.\n\n"

        f"Current Monitor Checked At:\n"
        f"{current_monitor_checked_at}\n\n"

        f"Last Monitor Checked At:\n"
        f"{last_monitor_checked_at}\n\n"

        f"Latest Update Timestamp:\n"
        f"{latest_update_timestamp}"
    )

last_checked_data = {

    "current_monitor_checked_at":
    current_monitor_checked_at,

    "last_monitor_checked_at":
    last_monitor_checked_at,

    "previous_update_timestamp":
    previous_update_timestamp,

    "latest_update_timestamp":
    latest_update_timestamp,

    "updates_detected":
    updates_detected
}

save_last_checked_log(
    last_checked_data
)

save_processing_history({

    "checked_at":
    current_monitor_checked_at,

    "last_monitor_checked_at":
    last_monitor_checked_at,

    "previous_update_timestamp":
    previous_update_timestamp,

    "latest_update_timestamp":
    latest_update_timestamp,

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

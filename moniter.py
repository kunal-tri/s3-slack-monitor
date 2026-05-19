import os
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

        if (
            "last_processed_timestamp"
            not in state
        ):

            state[
                "last_processed_timestamp"
            ] = None

        if (
            "processed_timestamps"
            not in state
        ):

            state[
                "processed_timestamps"
            ] = []

        return state

    except Exception:

        default_state = {

            "last_processed_timestamp":
            None,

            "processed_timestamps":
            []

        }

        save_current_state(default_state)

        return default_state

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

last_processed_timestamp = previous_state.get(
    "last_processed_timestamp"
)

processed_timestamps = previous_state.get(
    "processed_timestamps",
    []
)

monitor_checked_at = get_ist_string()

print(
    f"[{monitor_checked_at}] "
    f"Fetching monitoring logs..."
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

    # VALID FORMAT:
    # monitor/20260519_153000/file.json

    if len(parts) >= 3:

        folder_name = parts[1]

        ignored_folders = [
            "last_checked",
            "state",
            "history"
        ]

        if (
            folder_name not in ignored_folders
            and
            "_" in folder_name
            and
            len(folder_name) == 15
        ):

            timestamp_folders.add(
                folder_name
            )

timestamp_folders = sorted(
    timestamp_folders
)

print(
    f"Valid timestamps found: "
    f"{timestamp_folders}"
)

if last_processed_timestamp is None:

    new_timestamps = timestamp_folders

else:

    new_timestamps = [

        ts for ts in timestamp_folders

        if ts > last_processed_timestamp
    ]

updates_detected = False

slack_logs = []

processed_rows = []

latest_processed_timestamp = (
    last_processed_timestamp
)

if new_timestamps:

    updates_detected = True

    latest_processed_timestamp = (
        new_timestamps[-1]
    )

    print(
        f"New timestamps found: "
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

    processed_timestamps.extend(
        new_timestamps
    )

    save_current_state({

        "last_processed_timestamp":
        latest_processed_timestamp,

        "processed_timestamps":
        processed_timestamps

    })

    slack_text = (

        "*🚨 ETL PIPELINE UPDATE DETECTED*\n\n"

        f"🕒 Monitor Checked At:\n"
        f"{monitor_checked_at}\n\n"

        f"📌 Previous Processed Timestamp:\n"
        f"{last_processed_timestamp}\n\n"

        f"📌 Latest Processed Timestamp:\n"
        f"{latest_processed_timestamp}\n\n"

        + "\n\n".join(slack_logs)
    )

else:

    print("No new updates detected")

    slack_text = (

        "*✅ ETL MONITOR STATUS*\n\n"

        "No new updates detected.\n\n"

        f"🕒 Monitor Checked At:\n"
        f"{monitor_checked_at}\n\n"

        f"📌 Last Processed Timestamp:\n"
        f"{last_processed_timestamp}"
    )

last_checked_data = {

    "monitor_checked_at":
    monitor_checked_at,

    "latest_processed_timestamp":
    latest_processed_timestamp,

    "updates_detected":
    updates_detected,

    "processed_rows_count":
    len(processed_rows)
}

save_last_checked_log(
    last_checked_data
)

save_processing_history({

    "checked_at":
    monitor_checked_at,

    "latest_processed_timestamp":
    latest_processed_timestamp,

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

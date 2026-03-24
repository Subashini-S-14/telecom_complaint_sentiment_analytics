import requests
import json
import os
import time
import logging
from datetime import datetime

# ----------- CONFIGURATION -----------
API_ENDPOINT = "https://opendata.fcc.gov/resource/3xyp-aqkj.json"
BATCH_SIZE = 1000
SAVE_DIR = "../data_lake/bronze/telecom_complaints/"
CHECKPOINT_FILE = "progress_offset.txt"

# ----------- INITIAL SETUP -----------
os.makedirs(SAVE_DIR, exist_ok=True)

logging.basicConfig(
    filename="pipeline_ingestion.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ----------- OFFSET MANAGEMENT -----------
def read_offset():
    """Read last processed offset from file"""
    if os.path.isfile(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return int(f.read().strip())
    return 0


def write_offset(value):
    """Update offset after each batch"""
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(value))


# ----------- API CALL -----------
def get_batch(offset_value):
    """Fetch one batch of records from API"""
    params = {
        "$limit": BATCH_SIZE,
        "$offset": offset_value
    }

    for retry in range(3):
        try:
            response = requests.get(API_ENDPOINT, params=params, timeout=20)
            response.raise_for_status()
            return response.json()

        except Exception as error:
            print(f"Retry {retry + 1} failed: {error}")
            time.sleep(2)

    return None


# ----------- SAVE DATA -----------
def save_to_file(records, offset_value):
    """Save batch to JSON file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"batch_{timestamp}_offset_{offset_value}.json"
    full_path = os.path.join(SAVE_DIR, filename)

    with open(full_path, "w") as file:
        json.dump(records, file, indent=2)

    return full_path


# ----------- MAIN PIPELINE -----------
def run_ingestion():
    current_offset = read_offset()

    print(f"Starting ingestion from offset: {current_offset}")
    logging.info(f"Ingestion started at offset {current_offset}")

    while True:
        print(f"\nFetching records from offset {current_offset}...")

        batch = get_batch(current_offset)

        # If API failed completely
        if batch is None:
            logging.error(f"API failure at offset {current_offset}")
            return False

        # If no more data
        if len(batch) == 0:
            print("No more records found. Stopping.")
            break

        # Save data
        file_saved = save_to_file(batch, current_offset)

        print(f"Saved {len(batch)} records → {file_saved}")
        logging.info(f"Stored {len(batch)} records at offset {current_offset}")

        # Move to next batch
        current_offset += BATCH_SIZE
        write_offset(current_offset)

        # Avoid rate limit
        time.sleep(1)

    return True


# ----------- ENTRY POINT -----------
if __name__ == "__main__":
    status = run_ingestion()

    if status:
        print("\nIngestion finished successfully")
        logging.info("Pipeline completed successfully")

        # Optional reset
        write_offset(0)
    else:
        print("\nIngestion encountered errors")
        logging.error("Pipeline execution failed")
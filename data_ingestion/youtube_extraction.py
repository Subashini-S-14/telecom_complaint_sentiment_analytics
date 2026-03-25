import os
import json
import time
import logging
from datetime import datetime
from googleapiclient.discovery import build


# ================== PROJECT PATHS ==================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)

CONFIG_FILE = os.path.join(ROOT_DIR, "config.json")
OUTPUT_DIR = os.path.join(ROOT_DIR, "data_lake", "bronze", "youtube_data")
LOG_PATH = os.path.join(CURRENT_DIR, "ingestion.log")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ================== LOGGER ==================
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(levelname)s | %(asctime)s | %(message)s"
)


# ================== API KEY HANDLING ==================
def load_api_key():
    key = os.getenv("YOUTUBE_API_KEY")

    if not key and os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as file:
            config_data = json.load(file)
            key = config_data.get("YOUTUBE_API_KEY")

    if not key:
        raise RuntimeError("API key missing. Provide via ENV or config.json")

    return key


# ================== YOUTUBE CLIENT ==================
def create_client():
    api_key = load_api_key()
    return build("youtube", "v3", developerKey=api_key)


# ================== SEARCH CONFIG ==================
SEARCH_TERMS = [
    "telecom complaints india",
    "mobile signal problem",
    "network outage india",
    "broadband issues",
    "telecom customer problems"
]

RESULTS_PER_PAGE = 50
MAX_PAGE_LIMIT = 5


# ================== DATA EXTRACTION ==================
def extract_video_info(item, keyword):
    snippet = item.get("snippet", {})
    video_id = item.get("id", {}).get("videoId")

    return {
        "video_id": video_id,
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "published_time": snippet.get("publishedAt"),
        "channel": snippet.get("channelTitle"),
        "search_term": keyword
    }


def fetch_videos(youtube):
    collected_data = []

    for term in SEARCH_TERMS:
        print(f"\n🔎 Searching for: {term}")
        logging.info(f"Processing term: {term}")

        page_token = None

        for page_no in range(1, MAX_PAGE_LIMIT + 1):
            print(f"➡️ Fetching page {page_no}")

            response = None

            for retry in range(3):
                try:
                    response = youtube.search().list(
                        q=term,
                        part="snippet",
                        maxResults=RESULTS_PER_PAGE,
                        pageToken=page_token,
                        type="video"
                    ).execute()
                    break

                except Exception as err:
                    logging.warning(f"Retry {retry+1} failed: {err}")
                    time.sleep(2)

            if not response:
                logging.error(f"Skipping term '{term}' page {page_no}")
                continue

            items = response.get("items", [])

            for item in items:
                video_data = extract_video_info(item, term)
                collected_data.append(video_data)

            page_token = response.get("nextPageToken")

            if not page_token:
                print("⚠️ End of results")
                break

            time.sleep(1)

    return collected_data


# ================== SAVE OUTPUT ==================
def write_to_file(records):
    file_name = f"yt_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    full_path = os.path.join(OUTPUT_DIR, file_name)

    with open(full_path, "w") as file:
        json.dump(records, file, indent=2)

    print(f"\n💾 Stored {len(records)} records at:\n{full_path}")
    logging.info(f"Saved {len(records)} records")


# ================== MAIN FLOW ==================
def run_pipeline():
    print("🚀 Starting data ingestion...")

    yt_client = create_client()
    data = fetch_videos(yt_client)

    if data:
        write_to_file(data)
        print("✅ Process completed successfully")
    else:
        print("⚠️ No data fetched")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        print("❌ Pipeline failed:", e)
        logging.error(f"Fatal error: {e}")
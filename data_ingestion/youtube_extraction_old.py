import requests
import json
import os
import time
from datetime import datetime

# -------- CONFIG --------
API_KEY = "AIzaSyDCDlUzHA4w6l06Rkveq26w28AKMn-Qk4M"  
SEARCH_QUERY = "telecom issues india"
MAX_RESULTS = 5

BASE_DIR = "../data_lake/bronze/youtube_sentiment/"
os.makedirs(BASE_DIR, exist_ok=True)

# -------- FUNCTION: SEARCH VIDEOS --------
def fetch_videos(query):
    url = "https://www.googleapis.com/youtube/v3/search"

    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": MAX_RESULTS,
        "key": API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    return data


# -------- FUNCTION: GET COMMENTS --------
def fetch_comments(video_id):
    url = "https://www.googleapis.com/youtube/v3/commentThreads"

    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": 20,
        "key": API_KEY
    }

    response = requests.get(url, params=params)
    return response.json()


# -------- MAIN PIPELINE --------
def run_youtube_ingestion():
    print("🔍 Fetching videos...")

    video_data = fetch_videos(SEARCH_QUERY)

    all_data = []

    for item in video_data.get("items", []):
        video_id = item["id"]["videoId"]

        print(f"📥 Fetching comments for video: {video_id}")

        comments_data = fetch_comments(video_id)

        # Store full raw structure (no flattening)
        record = {
            "video_metadata": item,
            "comments_data": comments_data
        }

        all_data.append(record)

        time.sleep(1)  # avoid rate limit

    # -------- SAVE TO JSON --------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(BASE_DIR, f"youtube_raw_{timestamp}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Data saved to: {file_path}")
    print(f"📊 Total videos processed: {len(all_data)}")


# -------- RUN --------
if __name__ == "__main__":
    run_youtube_ingestion()
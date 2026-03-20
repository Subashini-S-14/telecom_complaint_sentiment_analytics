import requests

BASE_URL = "https://opendata.fcc.gov/resource/3xyp-aqkj.json"

keywords = ["robocall", "telemarketing", "spam"]

for keyword in keywords:

    params = {
        "$limit": 5,
        "$q": keyword
    }

    response = requests.get(BASE_URL, params=params)
    data = response.json()

    print(f"\nResults for keyword: {keyword}\n")

    for item in data:
        issue = item.get("issue", "N/A")
        issue_type = item.get("issue_type", "N/A")
        city = item.get("city", "N/A")
        state = item.get("state", "N/A")
        phone = item.get("caller_id_number", "N/A")

        print("Issue:", issue)
        print("Issue Type:", issue_type)
        print("Caller ID:", phone)
        print("Location:", city, state)
        print("-"*50)
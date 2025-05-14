import os
from dotenv import load_dotenv
import json
import time
import requests
from elasticsearch8 import Elasticsearch
import redis

load_dotenv()
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

elastic = Elasticsearch(hosts=[ELASTICSEARCH_URL])
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

LAST_SEEN_KEY = "last_seen_timestamp"

def get_last_seen_time():
    return r.get(LAST_SEEN_KEY)

def update_last_seen_time(ts):
    r.set(LAST_SEEN_KEY, ts)

def create_index():
    if not elastic.indices.exists(index=ELASTICSEARCH_INDEX):
        elastic.indices.create(index=ELASTICSEARCH_INDEX)

def find_logs_problematic(last_ts):
    must_conditions = []

    if last_ts:
        must_conditions.append({"range": {"timestamp": {"gt": last_ts}}})

    query = {
        "query": {
            "bool": {
                "must": must_conditions,
                "should": [
                    {"terms": {"status": [400, 401, 403, 404, 500]}},
                    {"range": {"duration": {"gt": 2}}}
                ],
                "minimum_should_match": 1
            }
        },
        "sort": [{"timestamp": "asc"}]
    }

    result = elastic.search(index=ELASTICSEARCH_INDEX, body=query)
    logs = result["hits"]["hits"]
    return logs

def send_alert_tg(text):
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    requests.post(url, json=payload)

def main():
    print("Alert Service started....")
    create_index()

    while True:
        last_seen = get_last_seen_time()
        problematic_logs = find_logs_problematic(last_seen)

        latest_ts = last_seen

        for log in problematic_logs:
            log_ts = log['_source']['timestamp']
            if last_seen and log_ts <= last_seen:
                continue

            message = f" *Problematic log:* ```\n{json.dumps(log['_source'], indent=2)}```"
            send_alert_tg(message)

            if not latest_ts or log_ts > latest_ts:
                latest_ts = log_ts

        if latest_ts:
            update_last_seen_time(latest_ts)

        time.sleep(10)

if __name__ == "__main__":
    main()

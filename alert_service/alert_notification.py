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


def is_seen(log_id):
  return r.sismember("seen_logs", log_id)


def mark_seen(log_id):
  r.sadd("seen_logs", log_id)

def create_index():
  if not elastic.indices.exists(index=ELASTICSEARCH_INDEX):
    print(f"Index {ELASTICSEARCH_INDEX} doesn't exist, creating a new one...")
    elastic.indices.create(index=ELASTICSEARCH_INDEX)
  else:
    print(f"Index {ELASTICSEARCH_INDEX} exists! ")


def find_logs_problematic():
  query = {
    "query": {
      "bool": {
        "should": [
          {"terms": {"status": [400, 401, 403, 404, 500]}},
          {"range": {"duration": {"gt": 2}}}
        ],
        "minimum_should_match": 1
      }
    }
  }

  result = elastic.search(index=ELASTICSEARCH_INDEX, body=query)
  return result["hits"]["hits"]

def send_alert_tg(text):
  url =  f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
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
        problematic_logs = find_logs_problematic()

        for log in problematic_logs:
            log_id = log["_id"]
            if not is_seen(log_id):
                message = f" *Problematic log:* ```\n{json.dumps(log['_source'], indent=2)}```"
                send_alert_tg(message)
                mark_seen(log_id)

        time.sleep(10)

if __name__ == "__main__":
    main()

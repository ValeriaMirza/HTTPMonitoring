import json
import time

import requests
from elasticsearch8 import Elasticsearch

ELASTICSEARCH_URL = "http://localhost:9200"
ELASTICSEARCH_INDEX = "monitoring-logs"
TELEGRAM_BOT_TOKEN = "8135071449:AAHcDej2ZWYz6Vj79DPQZbn-Pba6-Yq43-4"
TELEGRAM_CHAT_ID = "880872731"

elastic = Elasticsearch(hosts=[ELASTICSEARCH_URL])
seen_logs = set()

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
            if log_id not in seen_logs:
                message = f" *Problematic log:* ```\n{json.dumps(log['_source'], indent=2)}```"
                send_alert_tg(message)
                seen_logs.add(log_id)

        time.sleep(10)

if __name__ == "__main__":
    main()

from kafka import KafkaConsumer
import json
import requests

def main():
    consumer = KafkaConsumer(
        'logs_api',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='monitoring-group'
    )

    print("Listening for logs...")

    for msg in consumer:
        log = msg.value
        print("Received log:", log)

        try:
        
            es_url = "http://localhost:9200" 
            response = requests.post(f"{es_url}/monitoring-logs/_doc", json=log)

            if response.status_code == 201:
                print("Log indexed successfully.")
            else:
                print(f"Failed to index log. Status code: {response.status_code}")
                print(f"Response content: {response.text}")

        except requests.exceptions.RequestException as e:
            print("Error sending to Elasticsearch:", e)
        except Exception as e:
            print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()

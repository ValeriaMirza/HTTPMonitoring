import json
import time
import zipfile
from datetime import datetime, timedelta
import pytz
from elasticsearch import Elasticsearch
import paramiko
import os

ES_HOST = 'http://localhost:9200'
ES_INDEX = 'monitoring-logs'
SFTP_HOST = 'localhost'
SFTP_PORT = 2222
SFTP_USER = 'user'
SFTP_PASS = 'pass'
ARCHIVE_DIR = 'archives'
CHISINAU_TZ = pytz.timezone('Europe/Chisinau')

os.makedirs(ARCHIVE_DIR, exist_ok=True)

def flush_and_upload():
    es = Elasticsearch(ES_HOST)

    now = datetime.now(CHISINAU_TZ)
    print(f"Current time in Chisinau: {now}")
    one_minute_ago = now - timedelta(minutes=1)

    one_minute_ago_utc = one_minute_ago.astimezone(pytz.utc)

    query = {
        "size": 10000,
        "query": {
            "range": {
                "timestamp": {
                    "lt": one_minute_ago_utc.isoformat()
                }
            }
        }
    }

    res = es.search(index=ES_INDEX, body=query)
    hits = res['hits']['hits']
    if not hits:
        print("No logs to export.")
        return

    data = [doc['_source'] for doc in hits]
    ids_to_delete = [doc['_id'] for doc in hits]


    timestamp = now.strftime('%Y-%m-%d_%H-%M-%S')
    json_file = os.path.join(ARCHIVE_DIR, f'logs_{timestamp}.json')
    zip_file = os.path.join(ARCHIVE_DIR, f'logs_{timestamp}.zip')


    with open(json_file, 'w') as f:
        json.dump(data, f)

    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(json_file, os.path.basename(json_file))

   
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            sftp.chdir('/upload')
        except IOError:
            sftp.mkdir('/upload')
            sftp.chdir('/upload')

        sftp.put(zip_file, os.path.basename(zip_file))
        sftp.close()
        transport.close()
        print(f"Uploaded to SFTP: {zip_file}")
    except Exception as e:
        print(f"[SFTP Error] {e}")
        return

  
    for doc_id in ids_to_delete:
        try:
            es.delete(index=ES_INDEX, id=doc_id)
        except Exception as e:
            print(f"[Elasticsearch Delete Error] Failed to delete ID {doc_id}: {e}")


    try:
        os.remove(json_file)
        os.remove(zip_file)
        os.rmdir(ARCHIVE_DIR)
        print(f"Local files deleted: {json_file}, {zip_file}")
    except Exception as e:
        print(f"[Cleanup Error] Failed to delete local files: {e}")
 
    print(f"Archived and cleaned up {len(hits)} documents.")

def run_hourly():
    while True:
        flush_and_upload()
        time.sleep(3600)

if __name__ == '__main__':
    run_hourly()

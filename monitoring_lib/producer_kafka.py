from confluent_kafka import Producer
import json

class KafkaLogger:
  def __init__(self, kafka_server='localhost:9092', topic='logs_api'):
    self.producer = Producer({"bootstrap.servers": kafka_server})
    self.topic = topic

  def send_log(self, data):
    self.producer.produce(
      topic=self.topic,
      value=json.dumps(data).encode('utf-8')
    )
    self.producer.flush()

kafka_logger = KafkaLogger()



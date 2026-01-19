from kafka import KafkaProducer
import json

class ChatMessageProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def send(self, topic: str, user_id: str, data: dict):
        # Simple wrapper around KafkaProducer with user_id as key
        self.producer.send(topic, key=user_id, value=data)


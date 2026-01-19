from kafka import KafkaConsumer
from backend.kafka.prod.producer import ChatMessageProducer
import json
import time
from collections import defaultdict

BATCH_SECONDS = 600  # 10 Minuten

class ChatMessageConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            "chat.messages",
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k is not None else None
        )
        
        self.producer = ChatMessageProducer()

        self.buffers = defaultdict(list)
        self.last_flush = time.time()

    def start(self):
        for msg in self.consumer:
            self.collect(msg.key, msg.value)
            self.flush_if_over_10min()

    def flush_if_over_10min(self):
        if time.time() - self.last_flush >= BATCH_SECONDS:
            self.flush()
    
    def collect(self, user_id, data):
        self.buffers[user_id].append(data["message"])
        print(f"Append message from {user_id}: {data}")

    def flush(self):
        print("Flushing batches...")

        for user_id, messages in self.buffers.items():
            batch_message = " ".join(messages)

            data = {
                "user_id": user_id,
                "message": batch_message,
                "message_count": len(messages)
            }

            self.producer.send(
                "chat.messages.batch.analysis",
                user_id,
                data,
            )

            print(f"Send batched message from {user_id}: {batch_message}")

        self.buffers.clear()
        self.last_flush = time.time()


if __name__ == "__main__":
    consumer = ChatMessageConsumer()
    consumer.start()

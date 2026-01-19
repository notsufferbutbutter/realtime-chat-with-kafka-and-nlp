from datetime import datetime
from kafka import KafkaConsumer
from backend.kafka.prod.producer import ChatMessageProducer
import json
import psycopg2
from uuid import uuid4

connection = psycopg2.connect(
    host="127.0.0.1",
    port=55432,
    user="user",
    password="userpassword",
    dbname="PROD",
)

class ChatMessageAnalysisBatchConsumer:
    def __init__(self, db_connection):
        self.consumer = KafkaConsumer(
            "chat.messages.topic",
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k is not None else None
        )

        self.connection = db_connection

    def start(self):
        for msg in self.consumer:
            self.save_to_db(msg.value)

    def save_to_db(self, data: dict):
        user_id = data["user_id"]
        interests = data["interests"]

        for item in interests:
            topic = item["topic"]
            score = item["score"]

            cur = self.connection.cursor()

            cur.execute("""
                INSERT INTO message_topics (id, user_id, topic, score, analyzed_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (str(uuid4()), user_id, topic, score, datetime.utcnow()))

            self.connection.commit()

        print(f"Received message from {user_id}: {interests}")


if __name__ == "__main__":
    consumer = ChatMessageAnalysisBatchConsumer(connection)
    consumer.start()
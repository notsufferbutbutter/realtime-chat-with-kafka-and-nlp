import psycopg2
from uuid import uuid4
from kafka import KafkaProducer
import json
import time

TOPICS = ['travel', 'pet', 'car', 'bike',
        'clothes', 'hotel', 'flight', 
        'restaurant', 'investment',
        'bank', 'debt', 'book', 
        'cooking', 'furniture', 'dancing',
        'gym', 'sport', 'family', 'toy',
        'lottery', 'game']

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

connection = psycopg2.connect(
    host="127.0.0.1",
    port=55432,
    user="user",
    password="userpassword",
    dbname="DEV",
)
cur = connection.cursor()
cur.execute("""
            SELECT id, conversation_id, user_id, message, created_at
            FROM chat_messages
            ORDER BY created_at
        """)
rows = cur.fetchall()

print(f"LOGGING(test): {len(rows)} messages being analyzed in total")

# create users
for i, (message_id, conversation_id, user_id, message, created_at) in enumerate(rows):
    data = {
        "message_id": str(message_id),
        "conversation_id": str(conversation_id),
        "user_id": str(user_id),
        "message": message,
        "created_at": created_at.isoformat()
    }

    producer.send(
        "chat.messages",
        key=str(user_id),
        value=data
    )

    if i % 100 == 0:
        producer.flush()

    time.sleep(0.002)

producer.flush()


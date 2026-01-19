# Analysiere aller Messages innerhalb 15-30 Minuten Zeitfenster
from kafka import KafkaConsumer
from transformers import pipeline
from backend.kafka.prod.producer import ChatMessageProducer
import json

classifier = pipeline('zero-shot-classification', model='roberta-large-mnli')

candidate_labels = ['travel', 'pet', 'car', 'bike',
                    'clothes', 'hotel', 'flight', 
                    'restaurant', 'investment',
                    'bank', 'debt', 'book', 
                    'cooking', 'furniture', 'dancing',
                    'gym', 'sport', 'family', 'toy',
                    'lottery', 'game']

class ChatMessageAnalysisConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            "chat.messages.batch.analysis",
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8")
        )
        
        self.producer = ChatMessageProducer()

    def start(self):
        for msg in self.consumer:
            self.analyze_and_publish(msg.key, msg.value["message"])

    def analyze_and_publish(self, user_id: str, message: str):
        result = classifier(message[:1000], candidate_labels)
        topics = result["labels"][:3]
        scores = result["scores"][:3]

        interests = []
        for t, s in zip(topics, scores):
            interests.append({
                "topic": t,
                "score": float(s)
            })

        data = {
            "user_id": user_id,
            "interests": interests,
            "window": "10min"
        }

        self.producer.send(
            "chat.messages.topic",
            user_id,
            data,
        )

        print(f"Received message from {user_id}: {message}")


if __name__ == "__main__":
    consumer = ChatMessageAnalysisConsumer()
    consumer.start()
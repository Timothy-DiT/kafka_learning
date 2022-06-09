import json, time
from kafka import KafkaProducer
from data import get_registered_user

BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'registered_user_v2'

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers=[BROKER_URL],
                value_serializer=json_serializer
                )

if __name__ == "__main__":
    while True:
        registered_user = get_registered_user()
        print(registered_user)
        producer.send(TOPIC_NAME, registered_user)
        time.sleep(3)



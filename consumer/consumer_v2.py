import json
from kafka import KafkaConsumer

BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'registered_user_v2'

if __name__ == "__main__":
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BROKER_URL,
        auto_offset_reset='earliest',
        group_id='consumer_group_a'
    )
    print('starting the consumer')

    print(consumer)
    for msg in consumer:
        print(f"Registered User = {json.loads(msg.value)}")
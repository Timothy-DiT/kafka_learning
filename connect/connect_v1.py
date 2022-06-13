import asyncio
import requests, json
from confluent_kafka.admin import AdminClient, NewTopic


KAFKA_CONNECT_URL = "http://localhost:8083/connectors/"
BROKER_URL = 'PLAINTEXT://localhost:9092'
CONNECTOR_NAME = "connect_1"
TOPIC_NAME= "testing"
FILE_LOCATION = "/Users/dit/Documents/Learning/kafka/log.log"

def configure_connector():
    print("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")

    if resp.status_code == 200:
        print("\n\nSuccess\n\n")
        return

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
                {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "FsSourceConnector",
                    "topic": TOPIC_NAME,
                    "tasks.max": 1,
                    "fs.uris": FILE_LOCATION,
                    "key.converter.schemas.enable": "false",
                    "value.converter.schemas.enable": "false",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter"
                    }
                }
            )
    )

    print(resp.json())
    resp.raise_for_status()
    print('connector created successfully')

async def log():
    """Continually appends log to the end of file"""
    with open(FILE_LOCATION, 'w') as f:
        iteration = 0
        while True:
            f.write(f"log number {iteration}\n")
            f.flush()
            await asyncio.sleep(1.0)
            iteration += 1

async def log_task():
    """Runs the log task"""
    task = asyncio.create_task(log())
    configure_connector()
    await task

def run(topic):
    """Runs the simulation"""
    try:
        asyncio.run(log_task())
    except KeyboardInterrupt as e:
        print('Shutting down')
    finally:
        print('deleting')
        client.delete_topics([topic])



if __name__ == "__main__":
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    first_topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
    client.create_topics([first_topic])
    run(first_topic)
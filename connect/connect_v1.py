import asyncio
import requests, json


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "connect_1"
TOPIC_NAME= "testing"
FILE_LOCATION = "/Users/dit/Documents/Learning/kafka/log.log"


def configure_connector():
    print("creating or updating kafka connect connectore...")

    resps = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")

    if resps.status_code == 200:
        return

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
                {
                "name": CONNECTOR_NAME,
                "config": {
                    "name": "local-file-source",
                    "topic": TOPIC_NAME,
                    "connector.class": "FileStreamSource",
                    "tasks.max": 1,
                    "file": FILE_LOCATION,
                    "key.converter.schemas.enable": "false",
                    "value.converter.schemas.enable": "false",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter"
                    }
                }
            )
    )

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

def run():
    """Runs the simulation"""
    try:
        asyncio.run(log_task())
    except KeyboardInterrupt as e:
        print('Shutting down')


if __name__ == "__main__":
    run()
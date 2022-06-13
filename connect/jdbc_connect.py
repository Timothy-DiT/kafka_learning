import asyncio
import requests, json
from confluent_kafka.admin import AdminClient, NewTopic


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
BROKER_URL = 'PLAINTEXT://localhost:9092'
CONNECTOR_NAME = "postgres-jdbc"
TOPIC_NAME= "jdbc-"
FILE_LOCATION = "/Users/dit/Documents/Learning/kafka/log.log"
DB_CONNECT_URL = "jdbc:postgresql://localhost:5432/test_db"
DB_CONNECT_USER = "postgres"
DB_CONNECT_PASSWORD="sql67905355"
int_column='id'
db_table='yellow_taxi_data'

def configure_connector():
    print("creating or updating kafka connect connectore...")

    # jdbc_con = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")

    # if jdbc_con.status_code == 200:
    #     print("\n\nSuccess\n\n")
    #     return

    jdbc_con = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
                {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "topic.prefix": TOPIC_NAME,
                    "tasks.max": 1,
                    "mode": "incrementing",
                    "incrementing.column.name": int_column,
                    "table.whitelist": db_table,
                    "connection.url": DB_CONNECT_URL,
                    "connection.user": DB_CONNECT_USER,
                    "connection.password": DB_CONNECT_PASSWORD,
                    "key.converter.schemas.enable": "false",
                    "value.converter.schemas.enable": "false",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter"
                    }
                }
            )
    )

    print(jdbc_con.json())
    jdbc_con.raise_for_status()
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
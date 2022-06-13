from email import header
import io, asyncio, json, random, time
import requests
from faker import Faker
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import avro, Producer

from confluent_kafka.avro import (
                                AvroConsumer, 
                                AvroProducer, 
                                CachedSchemaRegistryClient)
from dataclasses import asdict, dataclass, field 



BROKER_URL = 'PLAINTEXT://localhost:9092'
SCHEMA_REGISTRY_URL = "http://localhost:8081"
REST_PROXY_URL = "http://localhost:8082"
TOPIC_NAME = 'rest_proxy_avro'
AVRO_SCHEMA = """{
        "type": "record",
        "name": "purchase",
        "fields": [
            {"name": "username", "type": "string"},
            {"name": "currency", "type": "string"},
            {"name": "amount", "type": "int"}
        ]
     }"""


faker = Faker()

@dataclass
class LineItem:
    description: str = field(default_factory=faker.bs)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    @classmethod
    def line_item(self):
        return[LineItem() for _ in range(random.randint(1, 10))]


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))
    line_items: list = field(default_factory=LineItem.line_item)




def produce():
    # https://docs.confluent.io/platform/current/kafka-rest/api.html
    headers = {
        "Content-Type": "application/vnd.kafka.avro.v2+json"
    }

    data = {
        "value_schema": AVRO_SCHEMA,
        "records": [
            {"value": asdict(Purchase()),}
        ]
    }

    resp = requests.post(
        f"{REST_PROXY_URL}/topics/{TOPIC_NAME}",
        headers=headers,
        data=json.dumps(data)
    )

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to send data to REST proxy {json.dumps(resp.json(), indent=2)}")
        return "Error"
    print(f"Sent data to REST Proxy {json.dumps(resp.json(), indent=2)}")


def main():
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        while True:
            response = produce()
            if response == 'Error':
                break
            time.sleep(0.5)
   
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        client.delete_topics([TOPIC_NAME])
        print('deleting')

if __name__ == "__main__":
    main()
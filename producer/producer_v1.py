import io, asyncio, json, random
from faker import Faker
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import avro, Producer

from confluent_kafka.avro import (
                                AvroConsumer, 
                                AvroProducer, 
                                CachedSchemaRegistryClient)
from dataclasses import asdict, dataclass, field 
from fastavro import parse_schema, writer

from data import get_registered_user


BROKER_URL = 'PLAINTEXT://localhost:9092'
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = 'registered_user_v1'


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

    schema = avro.loads("""{
        "type": "record",
        "name": "purchase",
        "namespace": "dit.avro.lesson",
        "fields": [
            {"name": "username", "type": "string"},
            {"name": "currency", "type": "string"},
            {"name": "amount", "type": "int"},
            {
                "name": "line_items",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "line_item",
                        "fields": [
                            {"name": "description", "type": "string"},
                            {"name": "amount", "type": "int"}
                        ]
                    }
                }
            }
        ]
     }"""
     )





async def produce(topic_name):

    schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)
    # create a produce
    p = AvroProducer({'bootstrap.servers': BROKER_URL},
                schema_registry=schema_registry
                )

    while True:
        # publish messages into the topic
        p.produce(
            topic=topic_name, 
            value=asdict(Purchase()),
            value_schema=Purchase.schema
        )
        await asyncio.sleep(3)


async def produce_consume():
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    await  t1


def main():
    # intialize a client
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    # create topic
    first_topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
    client.create_topics([first_topic])

    try:
        asyncio.run(produce_consume())
        print('here')
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        client.delete_topics([first_topic])
        print('deleting')

if __name__ == "__main__":
    main()
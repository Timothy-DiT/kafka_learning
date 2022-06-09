import asyncio
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from data import get_registered_user

BROKER_URL = 'PLAINTEXT://localhost:9092'
TOPIC_NAME = 'registered_user_v1'

def json_serializer(data):
    return json.dumps(data).encode("utf-8")


async def produce(topic_name):
    # create a produce
    p = Producer({'bootstrap.servers': BROKER_URL})

    while True:
        # publish messages into the topic
        registered_user = get_registered_user()
        p.produce(TOPIC_NAME, registered_user)
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
        print('deleting')
        client.delete_topics([first_topic])

if __name__ == "__main__":
    main()
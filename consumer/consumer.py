import asyncio
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = 'PLAINTEXT://localhost:9092'
TOPIC_NAME = 'registered_user_v2'


async def consume(topic_name):
    conf = {'bootstrap.servers': BROKER_URL,
        'group.id': 'consumer_group_a',
        'auto.offset.reset': 'earliest'}

    c = Consumer(conf)
    c.subscribe([TOPIC_NAME])


    while True:
        message = c.poll(1.0)

        if message is None:
            print('No message received')
        elif message.error() is not None:
            print(f"Message had an error {message.error}")
        else:
            print(f"Key: {message.key()}, Value: {message.value()}")
        
        await asyncio.sleep(1)



async def produce_consume():
    t1 = asyncio.create_task(consume(TOPIC_NAME))

    await  t1


def main():
    # intialize a client
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    # create topic


    try:
        asyncio.run(produce_consume())
        print('here')
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        print('deleting')

if __name__ == "__main__":
    main()
#!/usr/bin/python3

import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = 'PLAINTEXT://localhost:9092'
TOPIC_NAME = 'my-first-topic'

async def produce(topic_name):
    # create a produce
    p = Producer({'bootstrap.servers': BROKER_URL})

    curr_iteration = 0
    while True:
        # publish messages into the topic
        p.produce(TOPIC_NAME, f'Message: {curr_iteration}')
        curr_iteration += 1
        await asyncio.sleep(1)


async def consume(topic_name):
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "consumer_group_a"})
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
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))

    await  t1
    await  t2

client = AdminClient({'bootstrap.servers': BROKER_URL})
topic =  NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)


client.create_topics([topic])

try:
    asyncio.run(produce_consume())
except KeyboardInterrupt as e:
    print("shutting down")
finally:
    print('Deleting')
    client.delete_topics([topic])
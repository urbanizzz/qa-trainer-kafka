from confluent_kafka import Producer
import json
import time
from random import randrange


def run_producer(topic_name = 'test-topic', sleep = 3):
    conf = {'bootstrap.servers': "localhost:9092"}
    producer = Producer(conf)

    def delivery_report(err, msg):
        """ Вызывается один раз для каждого сообщения, чтобы подтвердить доставку """
        if err is not None:
            print(f'[Producer] Delivery error: {err}', flush=True)
        else:
            print(f'[Producer] The message in the topic {msg.topic()} ['
                  f'partition {msg.partition()} offset {msg.offset()}]', flush=True)

    print('[Producer] Producer is launched. Press Ctrl+C for exit.', flush=True)

    msg_number = 0

    try:
        while True:
            user_number = randrange(100)
            data = {'id': msg_number,
                    'user': f'user_{user_number:0>2}',
                    'action': f'test message #{msg_number}'}
            user_key = f'hash-{user_number * 2}'

            # Асинхронная отправка (попадает в локальную очередь)
            producer.produce(
                topic=topic_name,
                key = user_key,
                value = json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )

            # Опрашиваем события, чтобы сработал callback
            producer.poll(0)
            time.sleep(sleep)
            msg_number += 1

    finally:
        # Ждем завершения отправки всех накопленных сообщений
        producer.flush()

if __name__ == '__main__':
    run_producer()

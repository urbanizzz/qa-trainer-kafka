from confluent_kafka import Producer
import json
import time


def run_producer():
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
            data = {'id': msg_number, 'user': 'urban', 'action': f'test {msg_number}'}

            # Асинхронная отправка (попадает в локальную очередь)
            producer.produce(
                'test-topic',
                json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )

            # Опрашиваем события, чтобы сработал callback
            producer.poll(0)
            time.sleep(1)
            msg_number += 1

    finally:
        # Ждем завершения отправки всех накопленных сообщений
        producer.flush()

if __name__ == '__main__':
    run_producer()

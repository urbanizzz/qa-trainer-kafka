from confluent_kafka import Consumer, KafkaError
import json


def run_consumer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-first-group',        # ОБЯЗАТЕЛЬНО: точка в названии ключа
        'auto.offset.reset': 'earliest',     # Читать с начала, если группа новая
        'enable.auto.commit': True           # Автоматически сохранять позицию чтения
    }
    consumer = Consumer(conf)
    consumer.subscribe(['test-topic'])

    try:
        print('[Consumer] The consumer is launched and subscribed to '
              '"test-topic". Waiting...', flush=True)

        while True:
            msg = consumer.poll(1.0) # Ждем сообщение 1 секунду

            if msg is None:
                continue
            if msg.error():
                match msg.error().code():
                    case KafkaError.UNKNOWN_TOPIC_OR_PART:
                        print('[Consumer] Topic not created. Waiting for producer',
                              flush=True)
                        continue
                    case KafkaError._PARTITION_EOF:
                        continue
                    case _:
                        print(f'[Consumer] Ошибка: {msg.error()}', flush=True)
                        break

            # Десериализация и вывод
            data = json.loads(msg.value().decode('utf-8'))
            print(f'[Consumer] Message: {data} | Partition: {msg.partition()} '
                  f'| Offset: {msg.offset()}', flush=True)

    except KeyboardInterrupt:
        print('[Consumer] Consumer is stopping.', flush=True)
    except Exception as e:
        print(f'[Consumer] Critical error: {e}', flush=True)
    finally:
        if 'consumer' in locals():
            consumer.close()

if __name__ == '__main__':
    run_consumer()

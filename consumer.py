from confluent_kafka import Consumer, KafkaError
import json


def run_consumer(
        consumer_id = 0,
        topic_name = 'test-topic',
        group_id = 'test-group'
):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': group_id,               # ОБЯЗАТЕЛЬНО: точка в названии ключа
        'auto.offset.reset': 'earliest',    # Читать с начала, если группа новая
        'enable.auto.commit': True          # Автоматически сохранять позицию чтения
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic_name])

    try:
        print(f'[Consumer #{consumer_id}] The consumer is launched and '
              f'subscribed to "test-topic". Waiting...', flush=True)

        while True:
            msg = consumer.poll(1.0) # Ждем сообщение 1 секунду

            if msg is None:
                continue
            if msg.error():
                match msg.error().code():
                    case KafkaError.UNKNOWN_TOPIC_OR_PART:
                        print(f'[Consumer #{consumer_id}] Topic not created. '
                              f'Waiting for producer', flush=True)
                        continue
                    case KafkaError._PARTITION_EOF:
                        continue
                    case _:
                        print(f'[Consumer #{consumer_id}] Ошибка:'
                              f' {msg.error()}', flush=True)
                        break

            # Десериализация и вывод
            data = json.loads(msg.value().decode('utf-8'))
            print(f'[Consumer #{consumer_id}] Message: {data} | Partition:'
                  f' {msg.partition()} | Offset: {msg.offset()}', flush=True)

    except KeyboardInterrupt:
        print(f'[Consumer #{consumer_id}] Consumer is stopping.', flush=True)
    except Exception as e:
        print(f'[Consumer #{consumer_id}] Critical error: {e}', flush=True)
    finally:
        if 'consumer' in locals():
            consumer.close()

if __name__ == '__main__':
    run_consumer()

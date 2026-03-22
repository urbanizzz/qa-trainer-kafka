import multiprocessing
import time
import sys

from producer import run_producer
from consumer import run_consumer
from confluent_kafka.admin import AdminClient, NewTopic


TOPIC_NAME = 'my-test-topic'    # имя топика
GROUP_ID = 'my-test-group'      # имя группы
NUM_CONSUMERS = 3               # количество консюмеров и партиций
MSG_PAUSE = 3                   # Пауза между сообщениями в продюсере

def create_topic_if_not_exists(topic_name, num_partitions = 1):
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

    # Проверка есть ли уже такой топик
    metadata = admin_client.list_topics(timeout=5)
    if topic_name not in metadata.topics:
        print(f'[System] Creating topic {topic_name} with {num_partitions} '
              f'partitions...')
        new_topic = NewTopic(topic_name, num_partitions=num_partitions,
                             replication_factor=1)

        # Создаем топик
        fs = admin_client.create_topics([new_topic])
        # Ждём завершения операции
        for topic, f in fs.items():
            try:
                f.result()
                print(f'[System] Topic {topic} created successfully')
            except Exception as e:
                print(f'[System] Failed to create topic {topic}: {e}')
    else:
        print(f'[System] Topic {topic_name} already exists')

def start_consumer(id, topic, group):
    print(f'[System] Consumer # {id} is running...', flush=True)
    try:
        run_consumer(id, topic, group)
    except KeyboardInterrupt:
        pass

def start_producer(topic):
    print('[System] Producer is running...', flush=True)
    time.sleep(2)
    try:
        run_producer(topic, MSG_PAUSE)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    create_topic_if_not_exists(TOPIC_NAME, NUM_CONSUMERS)

    processes = []
    # Запускаем консюмеры
    for i in range(NUM_CONSUMERS):
        p = multiprocessing.Process(
            target=start_consumer,
            args=(i, TOPIC_NAME, GROUP_ID)
        )
        p.start()
        processes.append(p)
        print(f'[System] Consumer #{i} started.', flush=True)
    # Запускаем продюсер
    p_prod = multiprocessing.Process(
        target=start_producer,
        args=(TOPIC_NAME,)
    )
    p_prod.start()
    processes.append(p_prod)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print('\n[System] All processes is stopping...', flush=True)
        for p in processes:
            p.terminate()
        sys.exit(0)

import multiprocessing
import time
import sys

from producer import run_producer
from consumer import run_consumer


def start_consumer():
    print("[System] Consumer is running...", flush=True)
    try:
        run_consumer()
    except KeyboardInterrupt:
        pass


def start_producer():
    print("[System] Producer is running...", flush=True)
    time.sleep(2)
    try:
        run_producer()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    # Создаем два процесса
    p1 = multiprocessing.Process(target=start_consumer)
    p2 = multiprocessing.Process(target=start_producer)

    try:
        p1.start()
        p2.start()

        # Ждем завершения (или нажатия Ctrl+C)
        p1.join()
        p2.join()
    except KeyboardInterrupt:
        print("\n[System] All processes is stopping...", flush=True)
        p1.terminate()
        p2.terminate()
        sys.exit(0)


"""
"""
"""
pipeline.py
----------------
This file launches BOTH producer and consumer using Python threads to simulate
a real streaming pipeline.

"""

import os
import threading
from queue import Queue

from producer import CSVProducer
from consumer import Consumer

# Resolve the CSV next to this file, so the lab runs from any directory.
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "transactions.csv")

DELAY = 0.5          # seconds between two produced events
JITTER = 0.2         # random variation on that delay
PROCESS_TIME = 0.2   # simulated work per event in the consumer
MIN_AMOUNT = 0.0     # raise this to filter out small transactions


def main():
    """
    - Create the shared queue.
    - Instantiate the producer and the consumer.
    - Create a thread for each.
    - Start both threads and wait for them to finish.

    The students should run:
        python pipeline.py
    and observe live streaming behavior.
    """
    # The shared queue is our "topic": the only channel between the two threads.
    q = Queue()

    producer = CSVProducer(CSV_PATH, q, delay=DELAY, jitter=JITTER)
    consumer = Consumer(q, min_amount=MIN_AMOUNT, process_time=PROCESS_TIME)

    producer_thread = threading.Thread(target=producer.start, name="producer")
    consumer_thread = threading.Thread(target=consumer.start, name="consumer")

    # Start the consumer first so it is already listening when events arrive.
    consumer_thread.start()
    producer_thread.start()

    try:
        producer_thread.join()
        consumer_thread.join()
    except KeyboardInterrupt:
        # Ctrl+C: push the stop signal so the consumer can exit cleanly.
        print("\nInterrupted -- shutting the pipeline down...")
        q.put(None)
        consumer_thread.join()

    print("Pipeline finished.")


if __name__ == "__main__":
    print("Starting the Python Streaming Pipeline...")
    main()

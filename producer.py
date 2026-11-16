"""
producer.py
----------------
This file contains the logic for a Producer that simulates a real-time data
stream by reading events from a CSV file and sending them one by one into a
queue.

"""

import csv
import time
import random
from queue import Queue

# Special value pushed once the CSV is exhausted. It tells every consumer
# "there is nothing more coming, you may stop waiting".
SENTINEL = None


class CSVProducer:
    def __init__(self, csv_path, q, delay=1.0, jitter=0.0):
        """
        PARAMETERS:
        - csv_path : path to the CSV file (str)
        - q        : queue acting as our 'topic'
        - delay    : time to wait between sending each row (float)
        - jitter   : max random variation added to/removed from the delay,
                     used to imitate an irregular network (float)
        """
        self.csv_path = csv_path
        self.q = q
        self.delay = delay
        self.jitter = jitter
        self.sent = 0          # how many events we pushed into the queue

    def start(self):
        """
        Stream the CSV file into the queue, one row at a time.

        1. Open the CSV file.
        2. Loop through each row.
        3. Print the row being sent.
        4. Push it into the queue.
        5. Sleep for 'delay' seconds to simulate streaming.
        """
        try:
            with open(self.csv_path, newline="", encoding="utf-8") as f:
                # DictReader turns every row into a dict keyed by the header,
                # e.g. {"transaction_id": "1", "amount": "171.07", ...}
                reader = csv.DictReader(f)
                for row in reader:
                    print(f"[PRODUCER] -> {row}")
                    self.q.put(row)
                    self.sent += 1
                    time.sleep(self._next_delay())
        except FileNotFoundError:
            print(f"[PRODUCER] ERROR: CSV file not found: {self.csv_path}")
        finally:
            # Always signal the end of the stream, even if something failed,
            # otherwise the consumer would wait on an empty queue forever.
            self.q.put(SENTINEL)
            print(f"[PRODUCER] Finished. {self.sent} event(s) sent.")

    def _next_delay(self):
        """Delay before the next event, with optional random jitter."""
        if self.jitter:
            return max(0.0, self.delay + random.uniform(-self.jitter, self.jitter))
        return self.delay


# Debugging test

if __name__ == "__main__":
    """
    Run this file alone to test your producer.

    Expected behavior:
    - It should print rows from the CSV file every 'delay' seconds.
    """
    q = Queue()
    producer = CSVProducer("transactions.csv", q, delay=0.2, jitter=0.1)
    producer.start()
    # -1 because the queue also holds the SENTINEL.
    print(f"[TEST] Events waiting in the queue: {q.qsize() - 1}")

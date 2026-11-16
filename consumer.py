"""
consumer.py
----------------
This file contains the logic for a Consumer that continuously reads messages
from the queue and processes them.

The Consumer runs forever, just like a streaming system: it only stops when the
producer pushes the end-of-stream signal (None).
"""

import time
from queue import Queue


class Consumer:
    def __init__(self, q, min_amount=0.0, process_time=0.1):
        """
        PARAMETERS:
        - q            : the shared queue (simulated topic)
        - min_amount   : events below this amount are filtered out (float)
        - process_time : simulated work per event, in seconds (float)
        """
        self.q = q
        self.min_amount = min_amount
        self.process_time = process_time

        # State kept across events -> this is what makes the consumer
        # "stateful", exactly like a real streaming job.
        self.received = 0
        self.processed = 0
        self.skipped = 0
        self.total = 0.0

    def start(self):
        """
        Main loop of the consumer.

        - Continuously call q.get() to receive events (this blocks while the
          queue is empty, so the consumer never busy-waits).
        - Print the event received.
        - Pass it to process().
        - Stop when the end-of-stream signal (None) arrives.
        """
        print("[CONSUMER] Waiting for events...")
        while True:
            event = self.q.get()
            try:
                if event is None:
                    print("[CONSUMER] End-of-stream signal received.")
                    break
                self.received += 1
                print(f"[CONSUMER] <- {event}")
                self.process(event)
            finally:
                # Always mark the item as done, so q.join() cannot hang.
                self.q.task_done()
        self.report()

    def process(self, event):
        """
        Process a single event:
        - Simulate some processing time.
        - Transform fields (amount -> float).
        - Filter out events we are not interested in.
        - Update the running state (count + running total).
        """
        time.sleep(self.process_time)

        try:
            amount = float(event["amount"])
        except (KeyError, TypeError, ValueError):
            self.skipped += 1
            print(f"[CONSUMER]    skipped (bad or missing amount): {event}")
            return

        if amount < self.min_amount:
            self.skipped += 1
            print(f"[CONSUMER]    skipped (amount {amount:.2f} < {self.min_amount:.2f})")
            return

        self.processed += 1
        self.total += amount
        average = self.total / self.processed
        print(
            f"[CONSUMER]    user={event.get('user_id', '?')} "
            f"amount={amount:.2f} | count={self.processed} "
            f"total={self.total:.2f} avg={average:.2f}"
        )

    def report(self):
        """Print a small summary once the stream has ended."""
        average = self.total / self.processed if self.processed else 0.0
        print("[CONSUMER] ----- summary -----")
        print(f"[CONSUMER] received : {self.received}")
        print(f"[CONSUMER] processed: {self.processed}")
        print(f"[CONSUMER] skipped  : {self.skipped}")
        print(f"[CONSUMER] total    : {self.total:.2f}")
        print(f"[CONSUMER] average  : {average:.2f}")


# Debugging test

if __name__ == "__main__":
    """
    Run this alone to see consumer behavior.

    No producer is running here, so we hand-feed a few events and then the
    end-of-stream signal (None). Without that None the consumer would block
    forever waiting on the empty queue -- which is exactly what a real
    streaming consumer does.
    """
    q = Queue()
    q.put({"transaction_id": "1", "user_id": "103", "amount": "171.07"})
    q.put({"transaction_id": "2", "user_id": "104", "amount": "337.10"})
    q.put({"transaction_id": "3", "user_id": "105", "amount": "not-a-number"})
    q.put(None)

    consumer = Consumer(q)
    consumer.start()

import multiprocessing as mp
import time
import random


class Message:
    def __init__(self, data, ts=None):
        self.data = data
        self.ts = ts or time.time()


class Emitter:
    def __init__(self):
        self.targets = []  # multiple queues allowed

    def bind(self, queue):
        self.targets.append(queue)

    def emit(self, data):
        msg = Message(data)
        for q in self.targets:
            q.put_nowait(msg)
        print(f"[Emitter] Sent {data} to {len(self.targets)} queues")


class Receiver:
    def __init__(self):
        self.queue = None

    def bind(self, queue):
        self.queue = queue

    def read(self):
        try:
            msg = self.queue.get_nowait()
            print(f"[Receiver] Got {msg.data}")
        except mp.queues.Empty:
            pass


# --- Simulated World ---
class World:
    def __init__(self):
        self.connections = []

    def connect(self, emitter, receiver):
        q = mp.Queue(maxsize=5)
        emitter.bind(q)
        receiver.bind(q)
        self.connections.append(q)


# --- Example ---
if __name__ == "__main__":
    world = World()

    emitter = Emitter()
    receiver1 = Receiver()
    receiver2 = Receiver()

    # Connect emitter to two receivers → two separate queues
    world.connect(emitter, receiver1)
    world.connect(emitter, receiver2)

    # Emit a few messages
    for i in range(3):
        emitter.emit(random.randint(0, 100))
        time.sleep(0.5)

        receiver1.read()
        receiver2.read()

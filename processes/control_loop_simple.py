import multiprocessing as mp
import random
import time
from typing import Iterator


class Message:
    def __init__(self, data, ts=-1):
        self.data = data
        # set to current time if negative or not provided
        self.ts = ts if ts >= 0 else int(time.time() * 1e9)


# --- Emitter and Receiver ---
class Emitter:
    def __init__(self):
        self._queue = None  # will be set by World.connect

    def _bind(self, queue: mp.Queue):
        """Bind to a communication channel created by the World."""
        self._queue = queue

    def emit(self, data):
        if self._queue is not None:
            self._queue.put(Message(data))
        else:
            raise RuntimeError("Emitter not connected to any queue")


class Receiver:
    def __init__(self):
        self._queue = None  # will be set by World.connect

    def _bind(self, queue: mp.Queue):
        self._queue = queue

    def read(self):
        if self._queue is None:
            raise RuntimeError("Receiver not connected to any queue")

        try:
            return self._queue.get_nowait()
        except mp.queues.Empty:
            return None


# --- World: orchestrates connections and processes ---
class World:
    def __init__(self):
        self.processes = []

    def connect(self, emitter: Emitter, receiver: Receiver):
        """Creates a shared queue and binds both ends to it."""
        queue = mp.Queue(maxsize=10)
        emitter._bind(queue)
        receiver._bind(queue)

    def run_background(self, target):
        p = mp.Process(target=target)
        p.start()
        self.processes.append(p)

    def join_all(self):
        for p in self.processes:
            p.join()


# --- Sensor system ---
def sensor_loop(emitter: Emitter):
    for i in range(10):
        value = random.randint(0, 100)
        emitter.emit(value)
        print(f"[Sensor] Emitted {value}")
        time.sleep(0.5)


# --- Controller system ---
def controller_loop(receiver: Receiver):
    for _ in range(20):
        msg = receiver.read()
        if msg:
            print(f"[Controller] Received {msg.data} (ts={msg.ts})")
        time.sleep(0.2)


# --- Run the system ---
if __name__ == "__main__":
    world = World()

    sensor_emitter = Emitter()
    controller_receiver = Receiver()

    # World now connects them through a shared Queue
    world.connect(sensor_emitter, controller_receiver)

    # Run controller in a background process
    world.run_background(lambda: controller_loop(controller_receiver))

    # Run sensor in main process
    sensor_loop(sensor_emitter)

    # Wait for background to finish
    world.join_all()

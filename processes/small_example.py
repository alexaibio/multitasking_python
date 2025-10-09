import multiprocessing as mp
import random
import time
from collections import deque
from typing import Iterator

# --- Message class ---
class Message:
    def __init__(self, data, ts=-1):
        self.data = data
        self.ts = ts if ts >= 0 else int(time.time() * 1e9)

# --- Signal interfaces ---
class Emitter:
    def __init__(self):
        self.queue = deque(maxlen=10)

    def emit(self, data):
        self.queue.append(Message(data))

class Receiver:
    def __init__(self, emitter: Emitter):
        self.emitter = emitter

    def read(self):
        if self.emitter.queue:
            return self.emitter.queue.popleft()
        return None

# --- Simple "World" ---
class World:
    def __init__(self):
        self.processes = []

    def connect(self, emitter, receiver):
        return receiver  # in this simple example, receiver already has access to emitter

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
            print(f"[Controller] Received {msg.data}")
        time.sleep(0.2)

# --- Run the system ---
if __name__ == "__main__":
    world = World()

    # Create emitter and receiver
    sensor_emitter = Emitter()
    controller_receiver = Receiver(sensor_emitter)

    # Run controller in background
    world.run_background(lambda: controller_loop(controller_receiver))

    # Run sensor in main process
    sensor_loop(sensor_emitter)

    # Wait for background to finish
    world.join_all()

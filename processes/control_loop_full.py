"""
 - LocalQueueEmitter   -  Uses an in-memory `deque` → communication within the same process
 - MultiprocessEmitter -  Uses `multiprocessing.Queue` → communication between processes
 - BroadcastEmitter    -  Sends each message to several emitters at once
 - TransportMode       -  Enum that decides which transport to use (queue or shared memory)

"""

import multiprocessing as mp
import time
import random
from collections import deque
from enum import IntEnum


class Message:
    def __init__(self, data, ts=None):
        self.data = data
        self.ts = ts or time.time()


class TransportMode(IntEnum):
    UNDECIDED = 0
    QUEUE = 1
    SHARED_MEMORY = 2


class LocalQueueEmitter:
    def __init__(self, queue: deque):
        self._queue = queue

    def emit(self, data):
        self._queue.append(Message(data))
        print(f"[LocalEmitter] Sent {data} (in-process)")


class LocalQueueReceiver:
    def __init__(self, queue: deque):
        self._queue = queue
        self._last = None

    def read(self):
        if len(self._queue) > 0:
            self._last = self._queue.popleft()
        return self._last


class MultiprocessEmitter:
    def __init__(self, queue: mp.Queue, mode=TransportMode.UNDECIDED):
        self._queue = queue
        self._mode = mode

    def emit(self, data):
        if self._mode == TransportMode.UNDECIDED:
            self._mode = TransportMode.QUEUE
        self._queue.put(Message(data))
        print(f"[MPEmitter] Sent {data} via {self._mode.name}")


class MultiprocessReceiver:
    def __init__(self, queue: mp.Queue):
        self._queue = queue

    def read(self):
        try:
            msg = self._queue.get(timeout=1)
            return msg
        except Exception:
            return None


class BroadcastEmitter:
    def __init__(self, emitters):
        self.emitters = emitters

    def emit(self, data):
        print(f"[BroadcastEmitter] Broadcasting {data} to {len(self.emitters)} targets")
        for e in self.emitters:
            e.emit(data)


# -------------------------
# SensorLoop (Producer)
# -------------------------
def sensor_loop(emitter, name="Sensor"):
    """Simulate a periodic data producer."""
    for i in range(5):
        value = random.randint(0, 100)
        emitter.emit(value)
        print(f"[{name}] Emitted value {value}")
        time.sleep(1.0)  # 1 Hz


# -------------------------
# ControlLoop (Consumer)
# -------------------------
def control_loop(receiver, name="Controller"):
    """Simulate a periodic controller that reads and reacts."""
    for i in range(10):
        msg = receiver.read()
        if msg:
            print(f"[{name}] Received {msg.data} (ts={msg.ts:.2f}) → acting on it...")
        else:
            print(f"[{name}] Waiting for data...")
        time.sleep(0.5)  # 2 Hz


# -------------------------
# Demo World
# -------------------------
if __name__ == "__main__":
    local_queue = deque(maxlen=10)
    mp_queue = mp.Queue(maxsize=10)

    local_em = LocalQueueEmitter(local_queue)
    local_re = LocalQueueReceiver(local_queue)

    mp_em = MultiprocessEmitter(mp_queue)
    mp_re = MultiprocessReceiver(mp_queue)

    # Combine emitters into a broadcast (fan-out)
    broadcast = BroadcastEmitter([local_em, mp_em])

    # Start control loop in background (multiprocess)
    proc = mp.Process(target=control_loop, args=(mp_re, "BackgroundController"))
    proc.start()

    # Run sensor and local control in main process
    sensor_loop(broadcast, "MainSensor")
    control_loop(local_re, "LocalController")

    proc.join()
    print("[World] Finished.")

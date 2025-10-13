"""
 - LocalQueueEmitter   -  Uses an in-memory `deque` → communication within the same process
 - MultiprocessEmitter -  Uses `multiprocessing.Queue` → communication between processes
 - BroadcastEmitter    -  Sends each message to several emitters at once
 - TransportMode       -  Enum that decides which transport to use (queue or shared memory)

"""

import multiprocessing as mp
import time
from collections import deque
from enum import IntEnum


class Message:
    def __init__(self, data, ts=None):
        self.data = data
        self.ts = ts or time.time()


class TransportMode(IntEnum):
    UNDECIDED = 0
    QUEUE = 1
    SHARED_MEMORY = 2  # we won't implement SM, just show mode switching


class LocalQueueEmitter:
    """Simulates LocalQueueEmitter — local, fast, same process."""
    def __init__(self, queue: deque):
        self._queue = queue

    def emit(self, data):
        self._queue.append(Message(data))
        print(f"[LocalEmitter] Sent {data} (in-process)")


class LocalQueueReceiver:
    """Simulates LocalQueueReceiver — reads from local queue."""
    def __init__(self, queue: deque):
        self._queue = queue
        self._last = None

    def read(self):
        if len(self._queue) > 0:
            self._last = self._queue.popleft()
        return self._last



class MultiprocessEmitter:
    """Simulates MultiprocessEmitter — uses multiprocessing.Queue."""
    def __init__(self, queue: mp.Queue, mode=TransportMode.UNDECIDED):
        self._queue = queue
        self._mode = mode

    def emit(self, data):
        if self._mode == TransportMode.UNDECIDED:
            self._mode = TransportMode.QUEUE
        self._queue.put(Message(data))
        print(f"[MPEmitter] Sent {data} via {self._mode.name}")


class MultiprocessReceiver:
    """Simulates MultiprocessReceiver — runs in another process."""
    def __init__(self, queue: mp.Queue):
        self._queue = queue

    def read(self):
        try:
            msg = self._queue.get(timeout=1)
            print(f"[MPReceiver] Received {msg.data}")
        except Exception:
            pass


class BroadcastEmitter:
    """Broadcasts to multiple emitters."""
    def __init__(self, emitters):
        self.emitters = emitters

    def emit(self, data):
        print(f"[BroadcastEmitter] Broadcasting {data} to {len(self.emitters)} targets")
        for e in self.emitters:
            e.emit(data)


# -------------------------
# Demonstration
# -------------------------
def background_process(receiver: MultiprocessReceiver):
    """Run in separate process."""
    while True:
        receiver.read()


if __name__ == "__main__":
    # --- Local setup (same process) ---
    local_queue = deque(maxlen=10)
    local_em = LocalQueueEmitter(local_queue)
    local_re = LocalQueueReceiver(local_queue)

    # --- Multiprocess setup ---
    mp_queue = mp.Queue(maxsize=10)
    mp_em = MultiprocessEmitter(mp_queue)
    mp_re = MultiprocessReceiver(mp_queue)

    # Start background process to listen to mp_re
    proc = mp.Process(target=background_process, args=(mp_re,), daemon=True)
    proc.start()

    # --- Combine using BroadcastEmitter ---
    broadcast = BroadcastEmitter([local_em, mp_em])

    # --- Emit data ---
    for i in range(3):
        broadcast.emit(i)
        time.sleep(0.5)

        msg = local_re.read()
        if msg:
            print(f"[Main] Local receiver got {msg.data}")

    print("[Main] Done.")
    proc.terminate()

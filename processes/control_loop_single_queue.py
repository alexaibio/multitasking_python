import multiprocessing as mp
import random
import time


class Message:
    def __init__(self, data, ts=-1):
        self.data = data
        self.ts = ts if ts >= 0 else int(time.time() * 1e9)


class Emitter:
    def __init__(self):
        self._queue = None

    def _bind(self, queue: mp.Queue):
        self._queue = queue

    def emit(self, data):
        if self._queue is not None:
            self._queue.put(Message(data))
        else:
            raise RuntimeError("Emitter not connected to any queue")


class Receiver:
    def __init__(self):
        self._queue = None

    def _bind(self, queue: mp.Queue):
        self._queue = queue

    def read(self):
        if self._queue is None:
            raise RuntimeError("Receiver not connected to any queue")

        try:
            return self._queue.get_nowait()
        except mp.queues.Empty:
            return None


class World:
    def __init__(self):
        self.processes = []

    def __enter__(self):
        print("[World] Entered context")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print("[World] Exiting context...")
        self.join_all()
        print("[World] All background processes finished")

    def connect(self, emitter: Emitter, receiver: Receiver):
        queue = mp.Queue(maxsize=10)
        emitter._bind(queue)
        receiver._bind(queue)

    def run_background(self, target, *args):
        """Launch a target function with arguments in a background process."""
        p = mp.Process(target=target, args=args)
        p.start()
        print(f"[World] Started background process {p.name} (pid={p.pid})")
        self.processes.append(p)

    def join_all(self):
        for p in self.processes:
            p.join()
            print(f"[World] Process {p.name} finished")


# --- Sensor system ---
def sensor_loop(emitter: Emitter):
    for i in range(10):
        value = random.randint(0, 100)
        emitter.emit(value)
        print(f"[Sensor] Emitted {value}")
        time.sleep(1)


# --- Controller system ---
def controller_loop(receiver: Receiver):
    for _ in range(20):
        msg = receiver.read()
        if msg:
            print(f"[Controller] Received {msg.data} (ts={msg.ts})")
        time.sleep(0.2)


# --- Run the system ---
if __name__ == "__main__":
    with World() as world:
        sensor_emitter = Emitter()
        controller_receiver = Receiver()

        world.connect(sensor_emitter, controller_receiver)

        world.run_background(controller_loop, controller_receiver)

        sensor_loop(sensor_emitter)

import time
import random
from collections import deque
from dataclasses import dataclass
from threading import Thread
from typing import Iterator, Iterable


@dataclass
class Message:
    data: any
    ts: int = -1

@dataclass
class Sleep:
    seconds: float


class Emitter:
    """Emitter that can send messages to several receivers (fan-out)."""
    def __init__(self):
        self._queues: list[deque] = []

    def _bind(self, queue: deque):
        self._queues.append(queue)

    def emit(self, data):
        if not self._queues:
            raise RuntimeError("Emitter not connected to any receiver")
        msg = Message(data)
        for q in self._queues:
            if len(q) < q.maxlen:
                q.append(msg)


class Receiver:
    """Receiver that can only have one source (one emitter)."""
    def __init__(self):
        self._queue: deque | None = None

    def _bind(self, queue: deque):
        if self._queue is not None:
            raise RuntimeError("Receiver can only be connected to one emitter")
        self._queue = queue

    def read(self):
        if self._queue is None:
            raise RuntimeError("Receiver not connected to any emitter")
        if self._queue:
            return self._queue.popleft()
        return None



# --- Control loops ----------------------------------------------------------

def sensor_loop(emitter: Emitter, name: str, interval: float):
    """Background sensor producing messages periodically."""
    for i in range(5):
        value = random.randint(0, 100)
        emitter.emit((name, value))
        print(f"[{name}] Emitted {value}")
        time.sleep(interval)
    print(f"[{name}] Finished")


def controller_loop(receiver: Receiver, name: str) -> Iterator[Sleep]:
    """Foreground controller consuming messages cooperatively."""
    count = 0
    while count < 30:
        msg = receiver.read()
        if msg:
            sensor_name, value = msg.data
            print(f"[{name}] Got {value} from {sensor_name} (ts={msg.ts})")
        yield Sleep(0.2)
        count += 1
    print(f"[{name}] Finished")


# --- Cooperative world ------------------------------------------------------

class World:
    def __init__(self):
        self._background_threads: list[Thread] = []

    def __enter__(self):
        print("[World] Entered context")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print("[World] Exiting context...")
        for t in self._background_threads:
            t.join()
            print(f"[World] Background thread {t.name} finished")

    def connect(self, emitter: Emitter, receiver: Receiver):
        """Connect emitter and receiver via a local queue."""
        queue = deque(maxlen=10)
        emitter._bind(queue)
        receiver._bind(queue)
        print("[World] Connected emitter <-> receiver")

    def start_background(self, *sensor_loops):
        """Start background sensor loops in separate threads."""
        for loop_func in sensor_loops:
            t = Thread(target=loop_func, daemon=True)
            t.start()
            self._background_threads.append(t)
            print(f"[World] Started background thread {t.name}")

    def start(self, *, sensor_loops=(), controller_loops=()):
        """Run background sensors and cooperative controllers."""
        # Start background sensor threads
        self.start_background(*sensor_loops)

        # Run cooperative controllers in main thread
        print("[World] Starting cooperative foreground loops...")
        loops = list(controller_loops)

        while loops:
            still_running = []
            for loop in loops:
                try:
                    sleep_obj = next(loop)
                    if sleep_obj.seconds > 0:
                        time.sleep(sleep_obj.seconds)
                    still_running.append(loop)
                except StopIteration:
                    continue
            loops = still_running

        print("[World] All cooperative loops completed.")



if __name__ == "__main__":
    with World() as world:
        # Create sensors
        camera = Emitter()
        lidar_sensor = Emitter()

        # Create controllers
        camera_controller = Receiver()
        lidar_controller = Receiver()
        storage_controller = Receiver()


        # Connect each emitter to multiple receivers (fan-out)
        world.connect(camera, camera_controller)
        world.connect(camera, storage_controller)

        world.connect(lidar_sensor, lidar_controller)


        # Prepare background sensor loops
        sensor_loops = [
            lambda: sensor_loop(camera, "TempSensor", 1.0),
            lambda: sensor_loop(lidar_sensor, "LidarSensor", 1.5),
            lambda: sensor_loop(cam_sensor, "CameraSensor", 2.0),
        ]

        # Prepare cooperative controller loops
        controller_loops = [
            controller_loop(camera_controller, "MainController"),
            controller_loop(storage_controller, "Logger"),
        ]

        # Run everything
        world.start(sensor_loops=sensor_loops, controller_loops=controller_loops)

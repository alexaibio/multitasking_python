"""
The simple example of cooperative scheduling loop to process robotics loops.
Explains:
 - running sensor loops in separate threads
 - cooperative scheduling with generators
"""
import time
import random
from collections import deque
from dataclasses import dataclass
import threading as th
from typing import Iterator, Iterable


@dataclass
class Message:
    """That what sensors send to controllers"""
    data: any
    ts: int = -1

@dataclass
class Sleep:
    """A command to send from Sensor to Controller"""
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
def sensor_loop(stop_event, emitter: Emitter, name: str) -> Iterator[Sleep]:
    """Background Sensor loop with generator. Run in its own thread"""
    while not stop_event.is_set():
        sensor_reading = random.randint(0, 100)
        emitter.emit((name, sensor_reading))
        print(f"   --> Sensor [{name}] Emitted {sensor_reading}")
        yield Sleep(2)   # stop here and give up control to Controller by sendin a Sleep Command
    print(f"Sensor [{name}] Finished")


def controller_loop(stop_event, receiver: Receiver, name: str) -> Iterator[Sleep]:
    """Foreground controller consuming messages cooperatively."""
    while not stop_event.is_set():
        msg = receiver.read()
        if msg:
            sensor_name, value = msg.data
            print(f"Receiver [{name}] Got {value} from {sensor_name} (ts={msg.ts})")
            # ACTION: take an action based on sensor reading, maybe emit another message
        yield Sleep(5.0)
    print(f"Controller [{name}] Finished")



# --- Cooperative world context ------------------------------------------------------

class World:
    def __init__(self):
        self._stop_event = th.Event()
        self._background_threads: list[th.Thread] = []

    def __enter__(self):
        print("[World] Entered context")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Stop all background trheads at the end of Worls"""
        self.stop()
        print("[World] Exiting context...")
        for t in self._background_threads:
            t.join()    # wait until that thread’s function returns after stop signal
            print(f"[World] Background thread {t.name} finished")

    def stop(self):
        print("[World] Stopping sensors...")
        self._stop_event.set()

    def connect(self, emitter: Emitter, receiver: Receiver):
        """Connect emitter and receiver via a local queue."""
        queue = deque(maxlen=10)
        emitter._bind(queue)
        receiver._bind(queue)
        print("[World] Connected emitter <-> receiver")


    def _bg_sensor_wrapper(self, gen: Iterator[Sleep]):
        try:
            for cmd in gen:
                if isinstance(cmd, Sleep):
                    time.sleep(cmd.seconds)
                else:
                    raise ValueError(f"Unexpected command {cmd}")
        except StopIteration:
            pass
        print("[World] Sensor loop finished")  ### CHANGED


    def start(self, *, sensor_loops: list, controller_loops: list):
        """Run background sensors and cooperative controllers."""

        #### Start background sensor generator loops in separate threads
        for sensor_loop_gen in sensor_loops:
            t = th.Thread(target=self._bg_sensor_wrapper, args=(sensor_loop_gen,), daemon=True)
            t.start()
            self._background_threads.append(t)
            print(f"[World] Started background thread {t.name}")

        #### Start cooperative controllers in main thread
        while not self._stop_event.is_set():
            for loop in controller_loops:
                try:
                    command = next(loop)
                    if isinstance(command, Sleep):
                        time.sleep(command.seconds)
                    else:
                        raise ValueError(f" Wrong command {command}")
                except StopIteration:
                    continue

        print("[World] All cooperative loops completed.")


if __name__ == "__main__":
    with World() as world:
        # Create sensors
        camera = Emitter()
        lidar_sensor = Emitter()

        # Create controllers
        camera_controller = Receiver()
        image_storage_controller = Receiver()
        lidar_controller = Receiver()

        # Connect each emitter to multiple receivers (fan-out)
        world.connect(camera, camera_controller)
        world.connect(camera, image_storage_controller)
        world.connect(lidar_sensor, lidar_controller)

        sensor_loops = [
            sensor_loop(world._stop_event, camera, "TempSensor"),
            sensor_loop(world._stop_event, lidar_sensor, "LidarSensor"),
        ]

        controller_loops = [
            controller_loop(world._stop_event, camera_controller, "MainController"),
            controller_loop(world._stop_event, image_storage_controller, "Logger"),
        ]

        # Run World: reas all sensors in a real time
        world.start(sensor_loops=sensor_loops, controller_loops=controller_loops)


"""
The minimum example of cooperative scheduling with Global-clock world in robotics.

Note
 - all loops are running cooperatively inside a single thread, no separate thread/processes.
 - time is controlled by World ata certain FPS.
"""

import time
import random
from collections import deque
from dataclasses import dataclass
from typing import Iterator, Union, Callable


@dataclass
class Message:
    data: any
    ts: float = -1.0

@dataclass
class Sleep:
    seconds: float


class Emitter:
    def __init__(self):
        self._queues: list[deque] = []

    def _bind(self, queue: deque):
        self._queues.append(queue)

    def emit(self, data):
        msg = Message(data, ts=time.time())
        for q in self._queues:
            if len(q) < q.maxlen:
                q.append(msg)
            print(f"   --> Emitted {data}")


class Receiver:
    def __init__(self):
        self._queue: deque | None = None

    def _bind(self, queue: deque):
        self._queue = queue

    def read(self):
        if self._queue and self._queue:
            return self._queue.popleft()
        return None


# --- Cooperative coroutines with Sleep -------------------------------------

def sensor_loop(stop_check: Callable[[], bool], emitter: Emitter, name: str, interval: float) -> Iterator[Union[None, Sleep]]:
    """Sensor that emits data every `interval` seconds."""
    while not stop_check():
        reading = random.randint(0, 100)
        emitter.emit((name, reading))
        yield Sleep(interval)  # request next run after `interval` seconds


def controller_loop(stop_check: Callable[[], bool], receiver: Receiver, name: str, interval: float) -> Iterator[Union[None, Sleep]]:
    """Controller that processes messages every `interval` seconds."""
    while not stop_check():
        msg = receiver.read()
        if msg:
            sensor_name, value = msg.data
            print(f"[{name}] Got {value} from {sensor_name}")
        yield Sleep(interval)


# --- Global-Clock World ----------------------------------------------------

class World:
    """Global-clock cooperative scheduler with per-task sleep."""

    def __init__(self, fps: int = 10):
        self._stop = False
        self._fps = fps
        self._dt = 1.0 / fps  # global maximum step
        self._next_time_run: dict[Iterator, float] = {}

    def connect(self, emitter: Emitter, receiver: Receiver):
        queue = deque(maxlen=10)
        emitter._bind(queue)
        receiver._bind(queue)
        print("[World] Connected emitter <-> receiver")

    def stop(self):
        self._stop = True

    def is_stopped(self) -> bool:   # ← helper for readability
        return self._stop

    def run(self, *, sensor_loops: list[Iterator], controller_loops: list[Iterator]):
        """Run cooperative loops with per-task sleep until Ctrl+C."""
        print(f"[World] Starting global clock world at {self._fps} FPS (Ctrl+C to stop)...")

        all_loops = sensor_loops + controller_loops
        now = time.time()
        self._next_time_run = {loop: now for loop in all_loops}

        frame = 0
        try:
            while not self._stop:
                frame_start = time.time()
                now = frame_start

                # --- Run loops whose sleep has expired
                for loop in all_loops:
                    if now >= self._next_time_run[loop]:
                        try:
                            command = next(loop)
                            if isinstance(command, Sleep):
                                self._next_time_run[loop] = now + command.seconds
                            else:
                                # if coroutine yields None, just run next frame
                                self._next_time_run[loop] = now
                        except StopIteration:
                            # optional: remove finished loops
                            self._next_time_run.pop(loop)

                # --- Global frame pacing (do not exceed max FPS)
                elapsed = time.time() - frame_start
                sleep_time = max(0.0, self._dt - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)

                frame += 1

        except KeyboardInterrupt:
            print("\n[World] User interruption received (Ctrl+C).")
        finally:
            print(f"[World] Stopped after {frame} frames.")


# --- Main Simulation --------------------------------------------------------

if __name__ == "__main__":
    world = World(fps=10)  # maximum frame rate

    # Emitters (sensors)
    camera = Emitter()
    lidar = Emitter()

    # Receivers (controllers)
    camera_ctrl = Receiver()
    lidar_ctrl = Receiver()

    # Connect sensors to controllers
    world.connect(camera, camera_ctrl)
    world.connect(lidar, lidar_ctrl)

    # Define coroutines with individual update intervals
    sensor_loops = [
        sensor_loop(world.is_stopped, camera, "CameraSensor", interval=0.2),  # 5 Hz
        sensor_loop(world.is_stopped, lidar, "LidarSensor", interval=1.0),    # 1 Hz
    ]

    controller_loops = [
        controller_loop(world.is_stopped, camera_ctrl, "CameraController", interval=0.5),
        controller_loop(world.is_stopped, lidar_ctrl, "LidarController", interval=0.5),
    ]

    # Run world
    world.run(sensor_loops=sensor_loops, controller_loops=controller_loops)

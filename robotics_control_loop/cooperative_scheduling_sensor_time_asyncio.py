"""
Asyncio-based cooperative scheduling example (sensor-controlled world).

Note
 - All loops run cooperatively inside a single thread.
 - Each sensor and controller controls its own timing using asyncio.sleep().
 - The world just launches and connects them, no global clock.
"""
import asyncio
import random
from collections import deque
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Messaging primitives
# ---------------------------------------------------------------------------

@dataclass
class Message:
    """What sensors send to controllers."""
    data: any
    ts: float


class Emitter:
    """Emitter can send messages to several receivers (fan-out)."""
    def __init__(self):
        self._queues: list[deque] = []

    def _bind(self, queue: deque):
        self._queues.append(queue)

    async def emit(self, data):
        if not self._queues:
            raise RuntimeError("Emitter not connected to any receiver")
        msg = Message(data, ts=asyncio.get_event_loop().time())
        for q in self._queues:
            if len(q) < q.maxlen:
                q.append(msg)
        print(f"   --> Emitted {data}")


class Receiver:
    """Receiver that reads messages from one emitter."""
    def __init__(self):
        self._queue: deque | None = None

    def _bind(self, queue: deque):
        if self._queue is not None:
            raise RuntimeError("Receiver already connected")
        self._queue = queue

    def read(self):
        if self._queue and self._queue:
            return self._queue.popleft()
        return None


# ---------------------------------------------------------------------------
# Control loops (now async coroutines)
# ---------------------------------------------------------------------------

async def sensor_loop(stop_check, emitter: Emitter, name: str, interval: float):
    """Sensor that emits data periodically under its own timing."""
    while not stop_check():
        reading = random.randint(0, 100)
        await emitter.emit((name, reading))
        await asyncio.sleep(interval)
    print(f"[{name}] Sensor finished.")


async def controller_loop(stop_check, receiver: Receiver, name: str, interval: float):
    """Controller that consumes sensor messages periodically."""
    while not stop_check():
        msg = receiver.read()
        if msg:
            sensor_name, value = msg.data
            print(f"[{name}] Got {value} from {sensor_name} (ts={msg.ts:.2f})")
        await asyncio.sleep(interval)
    print(f"[{name}] Controller finished.")


# ---------------------------------------------------------------------------
# World
# ---------------------------------------------------------------------------

class World:
    """Asyncio world managing sensors and controllers cooperatively."""
    def __init__(self):
        self._stop = False

    def stop(self):
        self._stop = True
        print("[World] Stop signal received.")

    def is_stopped(self):
        return self._stop

    def connect(self, emitter: Emitter, receiver: Receiver):
        queue = deque(maxlen=10)
        emitter._bind(queue)
        receiver._bind(queue)
        print("[World] Connected emitter <-> receiver")

    async def run(self, *, sensor_tasks, controller_tasks):
        """Run sensors and controllers cooperatively under asyncio."""
        print("[World] Starting async cooperative world... (Ctrl+C to stop)")

        all_tasks = [
            asyncio.create_task(task) for task in (sensor_tasks + controller_tasks)
        ]

        try:
            await asyncio.gather(*all_tasks)
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            print("\n[World] User interruption received (Ctrl+C).")
        finally:
            self._stop = True
            print("[World] Stopped.")


# ---------------------------------------------------------------------------
# Main simulation
# ---------------------------------------------------------------------------

async def main():
    world = World()

    # Create emitters and receivers
    camera = Emitter()
    lidar = Emitter()
    camera_ctrl = Receiver()
    lidar_ctrl = Receiver()

    # Connect them
    world.connect(camera, camera_ctrl)
    world.connect(lidar, lidar_ctrl)

    # Define sensor + controller coroutines (each has its own sleep rate)
    sensor_tasks = [
        sensor_loop(world.is_stopped, camera, "CameraSensor", 0.5),
        sensor_loop(world.is_stopped, lidar, "LidarSensor", 1.0),
    ]

    controller_tasks = [
        controller_loop(world.is_stopped, camera_ctrl, "CameraController", 0.3),
        controller_loop(world.is_stopped, lidar_ctrl, "LidarController", 0.4),
    ]

    await world.run(sensor_tasks=sensor_tasks, controller_tasks=controller_tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Main] Interrupted by user.")

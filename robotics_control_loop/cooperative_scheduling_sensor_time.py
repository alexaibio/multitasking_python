"""
The simple example of cooperative scheduling loop to process robotics loops.

Explains:
 - running sensor loops in separate threads
 - cooperative scheduling with generators

Note
 - all loops are running cooperatively inside a single thread.
 - In this example time is controlled by sensors!
"""
import time
import random
from collections import deque
from dataclasses import dataclass
from typing import Iterator, Callable


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
def sensor_loop(stop_check: Callable[[], bool], emitter: Emitter, name: str) -> Iterator[Sleep]:
    """Background Sensor loop with generator. Run in its own thread"""
    while not stop_check():
        sensor_reading = random.randint(0, 100)
        emitter.emit((name, sensor_reading))
        print(f"   --> Sensor [{name}] Emitted {sensor_reading}")
        yield Sleep(2)   # stop here and give up control to Controller by sendin a Sleep Command
    print(f"Sensor [{name}] Finished")


def controller_loop(stop_check: Callable[[], bool], receiver: Receiver, name: str) -> Iterator[Sleep]:
    """Foreground controller consuming messages cooperatively."""
    while not stop_check():
        msg = receiver.read()
        if msg:
            sensor_name, value = msg.data
            print(f"Receiver [{name}] Got {value} from {sensor_name} (ts={msg.ts})")
            # ACTION: take an action based on sensor reading, maybe emit another message
        yield Sleep(5.0)    # Time is controlled here by individual loop not world!
    print(f"Controller [{name}] Finished")



# --- Cooperative world ------------------------------------------------------
class World:
    """Toy cooperative scheduler managing sensors and controllers separately."""
    def __init__(self):
        self._stop = False

    def stop(self):
        self._stop = True
        print("[World] Stop signal received.")

    def is_stopped(self) -> bool:   # ← helper for readability
        return self._stop

    def connect(self, emitter: Emitter, receiver: Receiver):
        queue = deque(maxlen=10)
        emitter._bind(queue)
        receiver._bind(queue)
        print("[World] Connected emitter <-> receiver")

    def start(self, *, sensor_loops: list[Iterator[Sleep]], controller_loops: list[Iterator[Sleep]]):
        """Run cooperative scheduling loop for both sensors and controllers."""
        print("[World] Starting cooperative world... (Ctrl+C to stop)")
        try:
            # Initialize scheduling of the next reading
            sensor_next_time = {loop: 0.0 for loop in sensor_loops}
            controller_next_time = {loop: 0.0 for loop in controller_loops}

            while not self._stop:
                now = time.time()

                #### Cooperative scheduling for sensors
                for s_loop in sensor_loops:
                    if now >= sensor_next_time[s_loop]:     # read only if its time has come
                        try:
                            command = next(s_loop)
                            if isinstance(command, Sleep):
                                sensor_next_time[s_loop] = now + command.seconds
                            else:
                                raise ValueError(f"Unexpected command: {command}")
                        except StopIteration:
                            # generator finished (provide no more iterations) - remove it from the list
                            sensor_next_time.pop(s_loop, None)
                            sensor_loops.remove(s_loop)
                            continue

                #### Cooperative scheduling for controllers
                for c_loop in controller_loops:
                    if now >= controller_next_time[c_loop]:
                        try:
                            command = next(c_loop)
                            if isinstance(command, Sleep):
                                controller_next_time[c_loop] = now + command.seconds
                            else:
                                raise ValueError(f"Unexpected command: {command}")
                        except StopIteration:
                            # generator finished (provide no more iterations) - remove it from the list
                            controller_next_time.pop(c_loop, None)
                            controller_loops.remove(c_loop)
                            continue

                # Prevent tight CPU spin
                time.sleep(0.01)

        except KeyboardInterrupt:
            print("\n[World] User interruption received (Ctrl+C).")
        finally:
            self._stop = True
            print("[World] Cooperative world stopped.")


# --- Main simulation --------------------------------------------------------

if __name__ == "__main__":
    world = World()

    # Create emitters and receivers
    camera = Emitter()
    lidar = Emitter()

    camera_controller = Receiver()
    lidar_controller = Receiver()

    # Connect emitters to receivers
    world.connect(camera, camera_controller)
    world.connect(lidar, lidar_controller)

    # Define coroutines
    sensor_loops = [
        sensor_loop(world.is_stopped, camera, "CameraSensor"),
        sensor_loop(world.is_stopped, lidar, "LidarSensor"),
    ]

    controller_loops = [
        controller_loop(world.is_stopped, camera_controller, "CameraController"),
        controller_loop(world.is_stopped, lidar_controller, "LidarController"),
    ]

    # Run everything cooperatively in one thread
    world.start(sensor_loops=sensor_loops, controller_loops=controller_loops)
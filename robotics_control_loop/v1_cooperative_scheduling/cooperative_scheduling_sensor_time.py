"""
The minimum example of cooperative scheduling with sensor-controlled world in robotics.

Note
 - all loops are running cooperatively inside a single thread, no separate thread/processes.
 - time is controlled by sensors, no global clock.
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
        self._queue: deque | None = None

    def _bind(self, queue: deque):
        self._queue = queue

    def emit(self, data):
        if self._queue is None:
            raise RuntimeError("Emitter not connected to any receiver")
        msg = Message(data)
        self._queue.append(msg)
        print(f"   --> Emitted {data}")


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
def sensor_emitter_loop(stop_check: Callable[[], bool], emitter: Emitter, name: str) -> Iterator[Sleep]:
    """Sensor loop with generator, might be run a background process later."""
    while not stop_check():
        sensor_reading = random.randint(0, 100)
        emitter.emit((name, sensor_reading))
        yield Sleep(2.0)   # stop here and give up control to Controller by sending a Sleep Command
    print(f"Sensor [{name}] Finished")


def controller_receiver_loop(stop_check: Callable[[], bool], receiver: Receiver, name: str) -> Iterator[Sleep]:
    """Foreground controller consuming messages cooperatively."""
    while not stop_check():
        msg = receiver.read()
        if msg:
            sensor_name, value = msg.data
            print(f"Receiver [{name}] Got {value} from {sensor_name} (ts={msg.ts})", end='')
            # ACTION: take an action based on sensor reading, maybe emit another message
            print("   ...do an ACTION based on sensor reading.")
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

    def local_pipe(self, emitter: Emitter, receiver: Receiver):
        queue = deque(maxlen=10)
        emitter._bind(queue)
        receiver._bind(queue)
        print("[World] Connected emitter <-> receiver")

    def run(self, *, sensor_loops: list[Iterator[Sleep]], controller_loops: list[Iterator[Sleep]]):
        """Run cooperative scheduling loop for both sensors and controllers."""
        print("[World] Starting cooperative world... (Ctrl+C to stop)")
        try:
            # Initialize scheduling of the next reading
            sensors_next_time = {s_loop: 0.0 for s_loop in sensor_loops}
            controllers_next_time = {c_loop: 0.0 for c_loop in controller_loops}

            while not self._stop:
                now = time.time()

                # both loops might be joined into one,
                # but I separated them to show that sensors might be run as processes/threads

                #### Cooperative scheduling for sensors (emitters)
                for s_loop in sensor_loops:
                    if now >= sensors_next_time[s_loop]:     # read sensor only if its time has come
                        try:
                            command = next(s_loop)
                            if isinstance(command, Sleep):
                                sensors_next_time[s_loop] = now + command.seconds
                            else:
                                raise ValueError(f"Unexpected command: {command}")
                        except StopIteration:
                            # generator finished (provides no more iterations) - remove it from the list
                            sensors_next_time.pop(s_loop, None)
                            sensor_loops.remove(s_loop)
                            continue

                #### Cooperative scheduling for controllers (receivers)
                for c_loop in controller_loops:
                    if now >= controllers_next_time[c_loop]:
                        try:
                            command = next(c_loop)
                            if isinstance(command, Sleep):
                                controllers_next_time[c_loop] = now + command.seconds
                            else:
                                raise ValueError(f"Unexpected command: {command}")
                        except StopIteration:
                            # generator finished (provide no more iterations) - remove it from the list
                            controllers_next_time.pop(c_loop, None)
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
    world.local_pipe(camera, camera_controller)
    world.local_pipe(lidar, lidar_controller)

    # Define coroutines
    sensor_loops = [
        sensor_emitter_loop(world.is_stopped, camera, "CameraSensor"),
        sensor_emitter_loop(world.is_stopped, lidar, "LidarSensor"),
    ]

    controller_loops = [
        controller_receiver_loop(world.is_stopped, camera_controller, "CameraController"),
        controller_receiver_loop(world.is_stopped, lidar_controller, "LidarController"),
    ]

    # Run everything cooperatively in one thread
    world.run(sensor_loops=sensor_loops, controller_loops=controller_loops)
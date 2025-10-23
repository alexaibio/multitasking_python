"""
 - LocalQueueEmitter   -  Uses an in-memory `deque` → communication between thread
 - MultiprocessEmitter -  Uses `multiprocessing.Queue` → communication between processes
 - BroadcastEmitter    -  Sends each message to several emitters at once
 - TransportMode       -  which transport to use (queue or shared memory)
"""

import sys
import time
import random
import threading
import numpy as np
from abc import ABC, abstractmethod
import multiprocessing as mp
from multiprocessing import shared_memory
from collections import deque
from enum import IntEnum
from dataclasses import dataclass
from typing import Generic, TypeVar, Sequence, List, Tuple, Any, Callable, Iterator, Dict
import select


T = TypeVar("T")


class TransportMode(IntEnum):
    UNDECIDED = 0
    QUEUE = 1
    SHARED_MEMORY = 2


class ParallelismType(IntEnum):
    THREAD = 1
    PROCESS = 2


@dataclass
class SensorSpec:
    id: str
    dtype: type
    read_fn: Callable[[], Any]
    transport: TransportMode
    interval: float = 1.0
    unit: str | None = None

### Define how each sensor gets its reading
def read_temp() -> float:
    return round(20 + random.uniform(-2, 5), 2)

def read_cloudiness() -> str:
    return random.choice(["Clear", "Partly Cloudy", "Cloudy", "Rain"])

def read_pressure() -> float:
    return round(1000 + random.uniform(-10, 10), 2)

def read_camera_sensor():
    return np.random.randint(0, 255, (64, 64, 3), dtype=np.uint8)

@dataclass
class Sleep:
    seconds: float

@dataclass
class Message(Generic[T]):
    data: T
    ts: int


class Clock:
    def now(self) -> float:
        return time.time()

    def now_ns(self) -> int:
        """Current timestamp in nanoseconds."""
        return time.time_ns()



#### Interfaces
class SignalEmitter(Generic[T]):
    def emit(self, data: T) -> bool:
        """Add data to a queue as a Message"""
        ...

class SignalReceiver(Generic[T]):
    def read(self) -> Message[T] | None:
        """Returns next message, otherwise last value. None if nothing was read yet."""
        ...

class ControlSystem(ABC):
    """
    A substitution for sensor_loop and controller_loop
     - ControlSystem is a task that runs inside the world’s main control loop.
     - each ControlSystem.run() acts like a coroutine that periodically yields to allow others to progress
    """
    @abstractmethod
    def run(self, should_stop: mp.Event, clock: Clock) -> Iterator[Sleep]:
        ...


####### Emitters / Receivers
class LocalQueueEmitter(SignalEmitter[T]):
    def __init__(self, queue: deque, clock: Clock):
        self.queue = queue
        self.clock = clock

    def emit(self, data: T):
        msg = Message(data=data, ts=self.clock.now_ns())       # TODO: better get it from World as parameter
        self.queue.append(msg)
        print(f"[Local Emitter] Emitted |{data}| at {msg.ts:.2f}\n")
        return True


class LocalQueueReceiver(SignalReceiver[T]):
    def __init__(self, queue: deque):
        self.queue = queue
        self.last_msg = None    # imagine sensor emits data every 2 seconds, but control loop runs every 0.1 seconds

    def read(self) -> Message[T] | None:
        if self.queue:
            self.last_msg = self.queue.popleft()
        return self.last_msg


class MultiprocessEmitter(SignalEmitter[T]):
    """Emitter for inter-process communication."""
    def __init__(self, queue: mp.Queue, clock: Clock, transport=TransportMode.QUEUE):
        self.queue = queue
        self.clock = clock
        self.transport = transport  # CHANGED: transport mode

    def emit(self, data: T) -> None:
        msg = Message(data=data, ts=self.clock.now())
        if self.transport == TransportMode.QUEUE:
            self.queue.put(msg)
        elif self.transport == TransportMode.SHARED_MEMORY:
            if isinstance(data, SMCompliant):
                size = data.buf_size()
                shared_mem = shared_memory.SharedMemory(create=True, size=size)
                data.set_to_buffer(shared_mem.buf)
                msg.data = shared_mem.name  # send shared memory name
                self.queue.put(msg)
            else:
                raise TypeError("Data must implement SMCompliant for shared memory")
        print(f"[MP Emitter] Emitted {data} at {msg.ts:.2f}\n")


class MultiprocessReceiver(SignalReceiver[T]):
    """Receiver for inter-process communication."""
    def __init__(self, queue: mp.Queue, transport=TransportMode.QUEUE, sm_class=None, *sm_args):
        self.queue = queue
        self.last_msg: Message[T] | None = None
        self.transport = transport
        self.sm_class = sm_class  # class to reconstruct shared memory object
        self.sm_args = sm_args

    def read(self) -> Message[T] | None:
        try:
            self.last_msg = self.queue.get_nowait()
            if self.transport == TransportMode.SHARED_MEMORY:
                if self.sm_class:
                    shared_mem = shared_memory.SharedMemory(name=self.last_msg.data)
                    sm_obj = self.sm_class(*self.sm_args)
                    sm_obj.read_from_buffer(shared_mem.buf)
                    shared_mem.close()
                    shared_mem.unlink()
                    self.last_msg.data = sm_obj
        except mp.queues.Empty:
            pass
        return self.last_msg


class BroadcastEmitter(SignalEmitter[T]):
    """
    Broadcasts one message to multiple emitters of the same type.
    does not manage its own queue.
    """
    def __init__(self, emitters: Sequence[SignalEmitter[T]]):
        self.emitters: Sequence[SignalEmitter[T]] = emitters

    def emit(self, data: T) -> None:
        for e in self.emitters:
            e.emit(data)


# ---------------------------------------------------------------------
# Control Systems
# ---------------------------------------------------------------------

class SensorSystem(ControlSystem):
    """Sensor emits numpy arrays via shared memory if transport mode is SHARED_MEMORY."""
    def __init__(self, sensor: SensorSpec, emitter: SignalEmitter):
        self.sensor = sensor
        self.emitter = emitter
        self.transport = sensor.transport

    def run(self, should_stop: mp.Event, clock: Clock) -> Iterator[Sleep]:
        while not should_stop.is_set():
            reading = self.sensor.read_fn()

            if self.transport == TransportMode.SHARED_MEMORY:
                # handle numpy data types
                if isinstance(reading, np.ndarray):
                    adapter = NumpySMAdapter(reading)
                    self.emitter.emit(adapter)
                elif isinstance(reading, dict):
                    adapter = NumpyDictAdapter(reading)
                    self.emitter.emit(adapter)
                else:
                    print(f"[WARN] Sensor {self.sensor.id}: data not SMCompliant, using queue fallback.")
                    self.emitter.emit((self.sensor.id, reading))
            else:
                # normal queue transport
                self.emitter.emit((self.sensor.id, reading))

            yield Sleep(self.sensor.interval)




class ControllerSystem(ControlSystem):
    """Controller reads from multiple sensor receivers."""
    def __init__(self, receivers: List[SignalReceiver]):
        self.receivers = receivers

    def run(self, should_stop: mp.Event, clock: Clock) -> Iterator[Sleep]:
        while not should_stop.is_set():
            # process each receiver once
            for receiver in self.receivers:
                msg = receiver.read()
                if msg:
                    sensor_name, value = msg.data
                    action = "COOL" if isinstance(value, (int, float)) and value > 25 else "HEAT"
                    print(f"[Controller] {sensor_name}: {value} → Action: {action}")
            yield Sleep(5.0)


def _bg_wrapper(control_system: ControlSystem, stop_event: mp.Event, clock: Clock):
    """
     - Ready to be sent to bg thread, run until stop_event is set.
     - Execute command returned from generator
    """
    try:
        gen = control_system.run(stop_event, clock)
        for cmd in gen:     # notice, we dont have next() here
            if isinstance(cmd, Sleep):
                time.sleep(cmd.seconds)
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[World] Stopping background {control_system.__class__.__name__}")



# -----------------------------
# SM: Shared Memory Support
# -----------------------------

class SMCompliant(ABC):
    """Interface for data that could be used as view of some contiguous buffer."""

    @abstractmethod
    def buf_size(self) -> int:
        pass

    @abstractmethod
    def set_to_buffer(self, buffer: memoryview | bytearray) -> None:
        """Serialize data to buffer."""
        pass

    @abstractmethod
    def read_from_buffer(self, buffer: memoryview | bytes) -> None:
        """Deserialize data from buffer."""
        pass

class NumpySMAdapter(SMCompliant):
    """Adapter for single numpy array."""
    def __init__(self, array: np.ndarray):
        # do not modify original array
        self.array = array.copy()

    def buf_size(self) -> int:
        return self.array.nbytes

    def set_to_buffer(self, buffer: memoryview | bytearray) -> None:
        # copy raw bytes of the numpy array into the target buffer without any conversions (fast)
        buffer[:self.array.nbytes] = self.array.tobytes()

    def read_from_buffer(self, buffer: memoryview | bytes) -> None:
        self.array[:] = np.frombuffer(buffer[:self.array.nbytes], dtype=self.array.dtype).reshape(self.array.shape)

class NumpyDictAdapter(SMCompliant):
    """Adapter for dictionary of numpy arrays."""
    def __init__(self, arrays: Dict[str, np.ndarray]):
        self.arrays = {k: np.copy(v) for k, v in arrays.items()}
        # Precompute buffer offsets
        self.offsets = {}
        offset = 0
        for k, arr in self.arrays.items():
            self.offsets[k] = offset
            offset += arr.nbytes
        self._size = offset

    def buf_size(self) -> int:
        return self._size

    def set_to_buffer(self, buffer: memoryview | bytearray) -> None:
        for k, arr in self.arrays.items():
            off = self.offsets[k]
            buffer[off:off+arr.nbytes] = arr.view(np.uint8)

    def read_from_buffer(self, buffer: memoryview | bytes) -> None:
        for k, arr in self.arrays.items():
            off = self.offsets[k]
            arr[:] = np.frombuffer(buffer[off:off+arr.nbytes], dtype=arr.dtype).reshape(arr.shape)



# ---------------------------------------------------------------------
class World:
    """ Scheduler: orchestrating control loops """
    def __init__(self):
        self._stop_event = mp.Event()
        self.background_processes = []
        self.clock = Clock()

    def _keypress_watcher(self):
        print("[World] Press any key to stop simulation...")
        while not self._stop_event.is_set():
            if select.select([sys.stdin], [], [], 0.1)[0]:
                sys.stdin.read(1)
                print("\n[World] Key pressed. Stopping...")
                self._stop_event.set()

    def new_local_pipe(self):
        """ Create data queue and assign it to both emitter and receiver """
        q = deque(maxlen=5)
        emitter = LocalQueueEmitter(q, self.clock)
        receiver = LocalQueueReceiver(q)
        return emitter, receiver

    # multiprocessing pipe creator
    def new_mp_pipe(self):
        q = mp.Queue(maxsize=5)
        emitter = MultiprocessEmitter(q, self.clock)
        receiver = MultiprocessReceiver(q)
        return emitter, receiver

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()


    def start(self,
              controller_systems: List[ControlSystem],
              bg_sensor_systems: List[ControlSystem]):
        """
        Start background sensor systems (each runs in its own thread/process) and then
        run controller systems cooperatively in main thread as coroutines.

        CHANGED: simplified API so start accepts lists of ControlSystem (not tuples of functions).
        """
        print("[world] is starting")

        # Start keypress watcher
        threading.Thread(target=self._keypress_watcher, daemon=True).start()

        ### Start background loops - independently, in separate threads/processes.
        for sensor_system in bg_sensor_systems:
            if PARALLELISM_TYPE == ParallelismType.PROCESS:
                pr = mp.Process(target=_bg_wrapper, args=(sensor_system, self._stop_event, self.clock))
                pr.start()
                self.background_processes.append(pr)
                print(f"[World] Started background process for {sensor_system.__class__.__name__}")
            else:
                thr = threading.Thread(target=_bg_wrapper, args=(sensor_system, self._stop_event, self.clock), daemon=True)
                thr.start()
                self.background_processes.append(thr)
                print(f"[World] Started background thread for {sensor_system.__class__.__name__}")


        #### Run main loop (cooperative scheduling) inside the main thread
        # CHANGED: run controller systems in cooperative round-robin by advancing their generators
        gens: List[Iterator[Sleep]] = []
        for cs in controller_systems:
            gens.append(cs.run(self._stop_event, self.clock))

        try:                    # handle KeyboardInterrupt
            while not self._stop_event.is_set() and gens:
                # copy to allow removal during iteration
                for g in list(gens):
                    try:
                        control_flow_command = next(g)
                        if isinstance(control_flow_command, Sleep):
                            time.sleep(control_flow_command.seconds)
                        else:
                            # unknown command: ignore or extend as needed
                            pass
                    except StopIteration:
                        # this controller finished, remove it
                        gens.remove(g)
                        continue
        except KeyboardInterrupt:
            pass
        finally:
            print("[World] Stopping...")
            self._stop_event.set()
            # join background threads/processes
            for pr in self.background_processes:
                pr.join()       # Wait for process/thread finish before continuing


# ---------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------
if __name__ == "__main__":
    PARALLELISM_TYPE = ParallelismType.PROCESS

    # Define sensors
    sensor_specs = [
        SensorSpec(id="temp_sensor", dtype=float, read_fn=read_temp, transport=TransportMode.QUEUE, interval=1.0,unit="°C"),
        SensorSpec(id="cloudy_sensor", dtype=str, read_fn=read_cloudiness, transport=TransportMode.QUEUE, interval=2.0),
        SensorSpec(id="pressure_sensor", dtype=float, read_fn=read_pressure, transport=TransportMode.QUEUE,interval=1.5, unit="hPa"),
        SensorSpec(
            id="camera_sensor",
            dtype=np.ndarray,
            read_fn=read_camera_sensor,
            transport=TransportMode.SHARED_MEMORY,
            interval=0.5,
        ),
    ]


    world = World()

    # create sensor pipes
    sensor_pipes = []
    for spec in sensor_specs:
        if PARALLELISM_TYPE == ParallelismType.PROCESS:
            q = mp.Queue(maxsize=5)
            emitter = MultiprocessEmitter(q, world.clock, transport=spec.transport)
            receiver = MultiprocessReceiver(q,
                                            spec.transport,
                                            NumpyDictAdapter if spec.transport == TransportMode.SHARED_MEMORY else None)
        else:
            emitter, receiver = world.new_local_pipe()

        sensor_pipes.append((spec, emitter, receiver))

    # create background loops
    bg_loops = [SensorSystem(spec, emitter) for spec, emitter, _ in sensor_pipes]
    receivers = [r for _, _, r in sensor_pipes]
    controller_loops = [ControllerSystem(receivers)]

    # start the world
    world.start(controller_loops, bg_loops)

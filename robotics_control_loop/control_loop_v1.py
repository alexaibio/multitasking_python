"""
Further development of cooperative scheduling loop, a minimum example.
Here has been added:
 - interface classes with extensive typing
 - interprocess handling: a possibility to run heavy sensors interprocess, add stop signal
 - shared memory transport instead of queue for multiprocess communication

New classes:
 - LocalQueueEmitter   -  Uses an in-memory `deque` → communication between thread
 - MultiprocessEmitter -  Uses `multiprocessing.Queue` → communication between processes
 - BroadcastEmitter    -  Sends each message to several emitters at once
 - TransportMode       -  which transport to use (queue or shared memory)
"""
import time
import random
import multiprocessing as mp
from multiprocessing import shared_memory
from collections import deque
from enum import IntEnum
from dataclasses import dataclass
from typing import Generic, TypeVar, Sequence, List, Tuple, Any, Callable
import select
import numpy as np
from abc import ABC, abstractmethod

T = TypeVar("T")

@dataclass
class Message(Generic[T]):
    data: T
    ts: float

class TransportMode(IntEnum):
    UNDECIDED = 0
    QUEUE = 1
    SHARED_MEMORY = 2

class ParallelismType(IntEnum):
    LOCAL = 0
    #THREAD = 1
    PROCESS = 2

@dataclass
class Sleep:
    seconds: float


class Clock:
    def now(self) -> float:
        return time.time()

    def now_ns(self) -> int:
        """Current timestamp in nanoseconds."""
        return time.time_ns()


############# SENSORS
@dataclass
class SensorSpec:
    id: str
    dtype: type
    read_fn: Callable[[], Any]
    transport: TransportMode
    interval: float = 1.0
    unit: str | None = None

# Functions to access sensors readings
def read_temp() -> float:
    return round(20 + random.uniform(-2, 5), 2)

def read_cloudiness() -> str:
    return random.choice(["Clear", "Partly Cloudy", "Cloudy", "Rain"])

def read_camera() -> np.ndarray:
    height = 320
    width = 200
    frame = np.random.randint(0, 256, (height, width, 3), dtype=np.uint8)
    return frame



#### Interfaces (Typing classes)
class SignalEmitter(Generic[T]):
    def emit(self, data: T) -> bool:
        """Add data to a queue as a Message"""
        ...

class SignalReceiver(Generic[T]):
    def read(self) -> Message[T] | None:
        """Returns next message, otherwise last value. None if nothing was read yet."""
        ...


### SM: Shared Memory Support
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
        self.array = array.copy()   # do not modify original array

    def buf_size(self) -> int:
        return self.array.nbytes

    def set_to_buffer(self, buffer: memoryview | bytearray) -> None:
        # copy raw bytes into the target buffer(fast)
        buffer[:self.array.nbytes] = self.array.tobytes()

    def read_from_buffer(self, buffer: memoryview | bytes) -> None:
        self.array[:] = np.frombuffer(buffer[:self.array.nbytes], dtype=self.array.dtype).reshape(self.array.shape)


####### Emitters / Receivers implementation, Local and interprocess
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


# Multiprocess emitter/receiver using multiprocessing.Queue
class MultiprocessEmitter(SignalEmitter[T]):
    """Emitter for inter-process communication."""
    def __init__(self, queue: mp.Queue, clock: Clock, transport=TransportMode.QUEUE):
        self.queue = queue
        self.clock = clock
        self.transport = transport

    def emit(self, data: T) -> None:
        msg = Message(data=data, ts=self.clock.now())

        if self.transport == TransportMode.QUEUE:
            # PUT() might block if maxsize exceeded
            # use put_nowait() for non-blocking behaviour
            self.queue.put(msg)
        elif self.transport == TransportMode.SHARED_MEMORY:
            if isinstance(data, SMCompliant):
                size = data.buf_size()      # create a shared memory block
                shared_mem = shared_memory.SharedMemory(create=True, size=size)
                data.set_to_buffer(shared_mem.buf)  # serialize data
                msg.data = shared_mem.name          # use queue to send shared memory name
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


# ---------------------------------------------------------------------
# Control Loop primitives
# ---------------------------------------------------------------------

def sensor_loop_gen(stop_event: mp.Event, emitter: SignalEmitter, sensor: SensorSpec):
    """
    Generator (NOTE: it cannot be sent to a thread because of serialization issue, need a wrapper!)
     - permanently read one sensor and emit its reading to queue
     - return a command to be executed in main cooperative loop
    """
    while not stop_event.is_set():
        reading = sensor.read_fn()
        emitter.emit((sensor.id, reading))  # emits labeled reading as tuple
        yield Sleep(1.0)                    # loop voluntarily pauses, World gets control back


def controller_loop_gen(stop_event: mp.Event, receivers: List[SignalReceiver]):
    """
    Generator: for now a single controller handles all sensors
    """
    while not stop_event.is_set():
        # get last_reading from all sensors, do actions then ask to sleep for 10 sec
        for sensor_idx, receiver in enumerate(receivers):  # iterate all sensors
            msg = receiver.read()
            if msg:
                sensor_name, value = msg.data
                action = "COOL" if value > 25 else "HEAT"
                # OPTIONAL: actuator_emitter.emit(action)
                print(f"  [Controller] Sensor [{sensor_name}] reading: {value:.2f}, Action: {action}")

        yield Sleep(10)      # loop voluntarily pauses, World gets control back


def _bg_wrapper_loop(sensor_loop_fn, stop_event, *args):
    """
     Needed because we cannot run sensor_loop_gen in a separate process
    """
    generator_fn = sensor_loop_fn(stop_event, *args)
    try:
        for command in generator_fn:
            if isinstance(command, Sleep):
                time.sleep(command.seconds)
            else:
                raise ValueError(f'Unknown command: {command}')
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[Background] Stopping {sensor_loop_fn.__name__}")


# ---------------------------------------------------------------------
class World:
    """ Scheduler: orchestrating control loops """
    def __init__(self):
        self._stop_event = mp.Event()
        self.background_processes = []
        self.clock = Clock()

    def local_pipe(self):
        """ Create data queue and assign it to both emitter and receiver """
        q = deque(maxlen=5)
        emitter = LocalQueueEmitter(q, self.clock)
        receiver = LocalQueueReceiver(q)
        return emitter, receiver

    # create interprocess pipe
    def mp_pipe(self):
        q = mp.Queue(maxsize=5)
        emitter = MultiprocessEmitter(q, self.clock)
        receiver = MultiprocessReceiver(q)
        return emitter, receiver

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    def start(self, controller_loops, bg_loops):
        print("[world] is starting")

        ### Start background loops (interprocess, not cooperative scheduled)
        for background_fn, args in bg_loops:
            if PARALLELISM_TYPE == ParallelismType.PROCESS:
                pr = mp.Process(target=_bg_wrapper_loop, args=(background_fn, self._stop_event, *args))
                pr.start()
                self.background_processes.append(pr)
                print(f"[World] Started background process for {background_fn.__name__}")
            else:
                # TODO: implement it
                raise ValueError(" LOCAL parallelism is not implemented yet")

        #### Run main loop (cooperative scheduling -  coroutines) inside the main process
        try:                    # handle KeyboardInterrupt
            while not self._stop_event.is_set():
                # allows handling multiple controllers (whole system)
                for cooperative_fn, args in controller_loops:
                    try:        # handle StopIteration when generator finishes
                        command = next(cooperative_fn(self._stop_event, *args)) # generator
                        # execute command: might also be Stor, Log - only control flow commands not actuators
                        if isinstance(command, Sleep):
                            time.sleep(command.seconds)
                        else:
                            raise ValueError(f" Wrong command {command}")
                    except StopIteration:
                        print(f'...Control loop generator  {cooperative_fn} stopped')
                        continue
        except KeyboardInterrupt:
            pass
        finally:
            print("[World] Stopping...")
            self._stop_event.set()
            for pr in self.background_processes:
                pr.join()       # Wait for process finish before continuing


# ---------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------
if __name__ == "__main__":
    PARALLELISM_TYPE = ParallelismType.LOCAL

    # Define sensors (with appropriate transport: queue or shared memory)
    sensor_specs: list[SensorSpec] = [
        SensorSpec(id="temp_sensor", dtype=float, read_fn=read_temp, interval=1.0, unit="°C", transport=TransportMode.QUEUE),
        SensorSpec(id="cloudy_sensor", dtype=str, read_fn=read_cloudiness, interval=2.0, transport=TransportMode.QUEUE),
        SensorSpec(id="camera", dtype=np.ndarray, read_fn=read_camera, interval=4, unit="frame", transport=TransportMode.SHARED_MEMORY),
    ]

    #### World Simulation
    world = World()         # CLOCK CREATED INSIDE WORLD to synchronize all sensors

    # create (emitter, receiver) pipe for sensors
    sensor_pipes: list[Tuple[SensorSpec, SignalEmitter, SignalReceiver]] = []

    # TODO: need to deside here which pipes are Local and which are interprocess
    # create background loops (for sensors)
    emitter: MultiprocessEmitter
    receiver: MultiprocessReceiver
    for sensor_spec in sensor_specs:
        emitter, receiver = world.mp_pipe()
        sensor_pipes.append((sensor_spec, emitter, receiver))

    bg_loops = [
        (sensor_loop_gen, (emitter, spec)) for spec, emitter, _ in sensor_pipes
    ]

    # create foregraund loops (for controller)
    receivers = [r for _, _, r in sensor_pipes]
    controller_loops = [
        (controller_loop_gen, (receivers,))
    ]

    # START simulation - run sensor robotics_control_loop in background and controllers loop cooperatively
    world.start(controller_loops, bg_loops)





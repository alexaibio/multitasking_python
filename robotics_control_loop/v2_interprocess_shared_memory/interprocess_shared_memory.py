"""
Further development of cooperative scheduling loop, a minimum example.
Here has been added:

"""
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from enum import IntEnum
import multiprocessing as mp
from multiprocessing import resource_tracker, shared_memory
from queue import Empty, Full
import random
import time
from typing import Any, Generic, List, Tuple, TypeVar

import numpy as np


T = TypeVar("T")

@dataclass
class Message(Generic[T]):
    data: T
    ts: float
    updated: bool = True


class TransportMode(IntEnum):
    QUEUE = 1
    SHARED_MEMORY = 2


class ParallelismType(IntEnum):
    LOCAL = 0
    MULTIPROCESS = 2


@dataclass
class Sleep:
    seconds: float


class Clock:
    def now(self) -> float:
        return time.time()

    def now_ns(self) -> int:
        return time.time_ns()


### SM: Shared Memory Support
class SMCompliant(ABC):
    """Any data to be sent to shared memory should comply"""

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

    @abstractmethod
    def instantiation_params(self) -> tuple:
        pass


class NumpySMAdapter(SMCompliant):
    """Adapter for single numpy array."""
    def __init__(self, shape: tuple[int, ...], dtype: np.dtype):
        self.array = np.empty(shape, dtype=dtype)

    def instantiation_params(self) -> tuple:
        """
        Receiving class expects AdapterClass(*adapter.instantiation_params()) to reconstruct the same array
        the data will be filled from shared memory then
        """
        return (self.array.shape, self.array.dtype)

    def buf_size(self) -> int:
        return self.array.nbytes

    def set_to_buffer(self, buffer: memoryview | bytearray) -> None:
        # copy raw bytes into the target buffer
        buffer[:self.array.nbytes] = self.array.tobytes()

    def read_from_buffer(self, buffer: memoryview | bytes) -> None:
        self.array[:] = np.frombuffer(buffer[:self.array.nbytes], dtype=self.array.dtype).reshape(self.array.shape)

    @staticmethod
    def lazy_init(array: np.ndarray, adapter: 'NumpySMAdapter | None') -> 'NumpySMAdapter':
        """Lazily initialize adapter and copy array data."""
        if adapter is None:
            adapter = NumpySMAdapter(array.shape, array.dtype)
        adapter.array[:] = array
        return adapter




############# SENSORS

class Sensor(ABC):
    def __init__(self, sensor_id: str, transport: TransportMode, interval: float = 1.0):
        self.sensor_id = sensor_id
        self.transport = transport
        self.interval = interval

    @abstractmethod
    def read(self) -> Any:
        """Read sensor value."""
        pass


class TemperatureSensor(Sensor):
    def __init__(self, sensor_id: str = "temp_sensor", transport: TransportMode = TransportMode.QUEUE):
        super().__init__(sensor_id, transport, interval=1.0)
        self.unit = "°C"

    def read(self) -> float:
        return round(20 + random.uniform(-2, 5), 2)


class CloudinessSensor(Sensor):
    def __init__(self, sensor_id: str = "cloudy_sensor", transport: TransportMode = TransportMode.QUEUE):
        super().__init__(sensor_id, transport, interval=2.0)

    def read(self) -> str:
        return random.choice(["Clear", "Partly Cloudy", "Cloudy", "Rain"])


class CameraSensor(Sensor):
    def __init__(self, sensor_id: str = "camera",
                 transport: TransportMode = TransportMode.SHARED_MEMORY,
                 width: int = 200, height: int = 320):
        super().__init__(sensor_id, transport, interval=4.0)
        self.width = width
        self.height = height
        self.unit = "frame"
        self._adapter: NumpySMAdapter | None = None     # reuse same adapter

    def read(self) -> NumpySMAdapter:
        frame = np.random.randint(0, 256, (self.height, self.width, 3), dtype=np.uint8)
        self._adapter = NumpySMAdapter.lazy_init(frame, self._adapter)
        return self._adapter


#### Interfaces
class SignalEmitter(Generic[T]):
    def emit(self, data: T) -> bool:
        """Add data to a queue as a Message"""
        ...

class SignalReceiver(Generic[T]):
    def read(self) -> Message[T] | None:
        """Returns next message, otherwise last value. None if nothing was read yet."""
        ...



####### Emitters / Receivers implementation, Local and interprocess
class LocalQueueEmitter(SignalEmitter[T]):
    def __init__(self, queue: deque, clock: Clock):
        self.queue = queue
        self.clock = clock

    def emit(self, data: T):
        msg = Message(data=data, ts=self.clock.now_ns())       # TODO: better get it from World as parameter
        self.queue.append(msg)

        print_friendly_data = msg.data[1] if not isinstance(msg.data[1], np.ndarray) else msg.data[1][0,0].tolist()
        print(f"[Local Emitter] Emitted |{print_friendly_data}| at {msg.ts:.2f}\n")
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

    def __init__(self,
                 transport: TransportMode,
                 queue: mp.Queue,
                 clock: Clock,
                 lock: mp.Lock = None,
                 ts_value: mp.Value = None,     # time of emitting fresh value
                 up_value: mp.Value = None,     # flaf of fresh SM block
                 sm_queue: mp.Queue = None):    # extra SM metadata queue
        self.transport = transport
        self.queue = queue
        self.clock = clock

        # Shared memory primitives and state
        self.lock = lock
        self.ts_value = ts_value
        self.up_value = up_value
        self.sm_queue = sm_queue
        self._sm: shared_memory.SharedMemory | None = None
        self._expected_buf_size: int | None = None

    def _emit_queue(self, data: T, ts: float) -> bool:
        """Send via regular queue."""
        msg = Message(data=data, ts=ts)
        try:
            self.queue.put_nowait(msg)
            return True
        except Full:
            try:
                self.queue.get_nowait()
                self.queue.put_nowait(msg)
                return True
            except (Empty, Full):
                return False

    def _emit_shared_memory(self, data: SMCompliant, ts: float) -> bool:
        """Send via shared memory."""
        # Validate data type
        assert isinstance(data, SMCompliant), f"SHARED_MEMORY mode requires SMCompliant data, got {type(data)}"

        buf_size = data.buf_size()

        # First time: create buffer and send metadata
        if self._sm is None:
            self._expected_buf_size = buf_size
            self._sm = shared_memory.SharedMemory(create=True, size=buf_size)

            # Send metadata once
            metadata = (self._sm.name, buf_size, type(data),
                        data.instantiation_params())
            self.sm_queue.put(metadata)
            print(f"[Emitter] Created SM buffer: {self._sm.name}, size={buf_size}")

        # Verify size consistency
        assert buf_size == self._expected_buf_size, \
            f"Buffer size changed: {buf_size} != {self._expected_buf_size}"

        # Write data with lock
        with self.lock:
            if not isinstance(data, NumpySMAdapter):
                raise ValueError(" Trying to send not SMComplient data to shared memory")
            data.set_to_buffer(self._sm.buf)
            self.ts_value.value = int(ts)
            self.up_value.value = True

        return True

    def emit(self, data: T) -> None:
        ts = self.clock.now_ns()

        if self.transport == TransportMode.SHARED_MEMORY:
            self._emit_shared_memory(data, ts)
        else:                           # TransportMode.QUEUE
            self._emit_queue(data, ts)

    def close(self) -> None:
        """Clean up shared memory."""
        if self._sm is not None:
            try:
                self._sm.close()    # stop using it
                self._sm.unlink()   # tell OS to delete the block
            except:
                pass    # process might not own the SM (only the creator should unlink)


class MultiprocessReceiver(SignalReceiver[T]):
    def __init__(self,
                 transport: TransportMode,
                 queue: mp.Queue,
                 lock: mp.Lock = None,
                 ts_value: mp.Value = None,
                 up_value: mp.Value = None,
                 sm_queue: mp.Queue = None):
        self.transport = transport
        self.queue = queue

        # Shared memory primitives and state
        self.lock = lock
        self.ts_value = ts_value
        self.up_value = up_value
        self.sm_queue = sm_queue
        self._sm: shared_memory.SharedMemory | None = None
        self._out_value: SMCompliant | None = None          # whether it already attached
        self._readonly_buffer: memoryview | None = None

        self.last_msg: Message[T] | None = None

    def _ensure_shared_memory_initialized(self) -> bool:
        """
        Lazy initialization from metadata queue.
        lazily attaches to a shared memory block created by another process.
        """

        # is SM already attached?
        if self._out_value is not None:
            return True

        # Reads metadata from a queue
        try:
            # data_type: class that interprets the bytes,
            # instantiation_params: parameters for that class
            sm_name, buf_size, data_type, instantiation_params = self.sm_queue.get_nowait()
        except Empty:   # queue is empty
            return False

        # Attach Receiver to existing shared memory
        self._sm = shared_memory.SharedMemory(name=sm_name)

        # The creator process is responsible for unlinking it, not the receiver - so we only attach to it!
        try:
            resource_tracker.unregister(self._sm._name, 'shared_memory')
        except:
            pass

        # Create read-only view (so this receiver can't modify shared data)
        self._readonly_buffer = self._sm.buf.toreadonly()[:buf_size]

        # Instantiates a local object that knows how to interpret the shared buffer
        # reuse self._out_value
        self._out_value = data_type(*instantiation_params) if self._out_value is None else self._out_value
        self._out_value.read_from_buffer(self._readonly_buffer)

        print(f"[Receiver] Attached to SM: {sm_name}")
        return True

    def _read_queue(self) -> Message[T] | None:
        """Read from regular queue."""
        try:
            self.last_msg = self.queue.get_nowait()
            if self.last_msg:
                self.last_msg.updated = True
            return self.last_msg
        except Empty:
            if self.last_msg:
                return Message(self.last_msg.data, self.last_msg.ts, False)
            return None

    def _read_shared_memory(self) -> Message[T] | None:
        # Check if anything was written yet
        with self.lock:
            if self.ts_value.value == -1:
                return None

        # Initialize if needed
        if not self._ensure_shared_memory_initialized():
            return None

        # Read with lock
        with self.lock:
            self._out_value.read_from_buffer(self._readonly_buffer)
            updated = self.up_value.value
            self.up_value.value = False

            return Message(
                data=self._out_value,
                ts=float(self.ts_value.value),
                updated=updated
            )

    def read(self) -> Message[T] | None:
        if self.transport == TransportMode.SHARED_MEMORY:
            return self._read_shared_memory()
        else:  # TransportMode.QUEUE
            return self._read_queue()

    def close(self) -> None:
        if self._readonly_buffer is not None:
            self._readonly_buffer.release()
        if self._sm is not None:
            self._sm.close()

# ---------------------------------------------------------------------
# Control Loop primitives
# ---------------------------------------------------------------------

# helper function to check stop condition
def should_stop(stop_event) -> bool:
    if stop_event is None:
        return False
    elif hasattr(stop_event, 'is_set'):
        return stop_event.is_set()
    else:
        return stop_event


def sensor_loop_gen(stop_event: mp.Event, emitter: SignalEmitter, sensor: Sensor):
    """
    Generator (NOTE: it cannot be sent to a thread because of serialization issue, need a wrapper!)
     - permanently read one sensor and emit its reading to queue
     - return a command to be executed in main cooperative loop
    """

    while not should_stop(stop_event):
        reading = sensor.read()
        emitter.emit(reading)
        yield Sleep(sensor.interval)


def controller_loop_gen(stop_event: mp.Event,
                        sensor_receivers: List[Tuple[Sensor, SignalReceiver]]):
    """Controller loop - reads from sensors and acts."""
    while not should_stop(stop_event):
        for sensor, receiver in sensor_receivers:
            msg = receiver.read()
            if msg:
                value = msg.data

                # Extract from adapter if needed
                if isinstance(value, NumpySMAdapter):
                    value = value.array

                # Format display value
                display_value = value if not isinstance(value, np.ndarray) \
                    else f"array[0,0]={value[0, 0]}"

                status = "FRESH" if msg.updated else "STALE"
                print(f"  [Controller] {sensor.sensor_id}: {display_value} [{status}]")

                if msg.updated:
                    print(f"    → Action based on {sensor.sensor_id}")

        yield Sleep(5)


def _bg_wrapper_loop(sensor_loop_fn, stop_event, *args):
    """
     Needed because we cannot run sensor_loop_gen in a separate process (serialization issue)
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

        self._manager = mp.Manager()
        self._cleanup_resources = []

    def local_pipe(self):
        """ Create data queue and assign it to both emitter and receiver """
        q = deque(maxlen=5)
        emitter = LocalQueueEmitter(q, self.clock)
        receiver = LocalQueueReceiver(q)
        return emitter, receiver

    # create interprocess pipe
    def mp_pipe(self, transport: TransportMode):
        message_queue = self._manager.Queue(maxsize=5)

        if transport == TransportMode.SHARED_MEMORY:
            # Create SM primitives
            lock = self._manager.Lock()
            ts_value = self._manager.Value('Q', -1)
            up_value = self._manager.Value('b', False)
            sm_queue = self._manager.Queue()

            emitter = MultiprocessEmitter(
                transport, message_queue, self.clock,
                lock, ts_value, up_value, sm_queue
            )
            receiver = MultiprocessReceiver(
                transport, message_queue,
                lock, ts_value, up_value, sm_queue
            )
        else:  # TransportMode.QUEUE
            emitter = MultiprocessEmitter(
                transport, message_queue, self.clock
            )
            receiver = MultiprocessReceiver(
                transport, message_queue
            )

        self._cleanup_resources.append((emitter, receiver))
        return emitter, receiver

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    def start(self, controller_loops, bg_loops):
        print("[world] is starting")

        ### Start background interprocess loops
        for background_fn, args in bg_loops:
            pr = mp.Process(target=_bg_wrapper_loop, args=(background_fn, self._stop_event, *args))
            pr.start()
            self.background_processes.append(pr)
            print(f"[World] Started background process for {background_fn.__name__}")

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

            # Cleanup
            for emitter, receiver in self._cleanup_resources:
                receiver.close()
                emitter.close()

            for pr in self.background_processes:
                pr.join()


# ---------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------
if __name__ == "__main__":
    PARALLELISM_TYPE = ParallelismType.MULTIPROCESS

    # Define sensors (with appropriate transport: queue or shared memory)
    sensors: list[Sensor] = [
        TemperatureSensor(sensor_id="temp_sensor", transport=TransportMode.QUEUE),
        CloudinessSensor(sensor_id="cloudy_sensor", transport=TransportMode.QUEUE),
        CameraSensor(sensor_id="camera", transport=TransportMode.SHARED_MEMORY),
    ]
    # VALIDATION: Ensure SM only used in MULTIPROCESS mode
    sm_sensors = [s for s in sensors
                  if s.transport == TransportMode.SHARED_MEMORY]
    if sm_sensors and PARALLELISM_TYPE == ParallelismType.LOCAL:
        raise ValueError(
            f"SHARED_MEMORY transport not allowed in LOCAL mode. "
            f"Sensors using SM: {[s.sensor_id for s in sm_sensors]}"
        )

    #### World Simulation
    world = World()         # CLOCK CREATED INSIDE WORLD to synchronize all sensors

    # create (emitter, receiver) pipe for sensors
    sensor_pipes: list[Tuple[Sensor, SignalEmitter, SignalReceiver]] = []
    for sensor_spec in sensors:
        if PARALLELISM_TYPE == ParallelismType.LOCAL:
            emitter, receiver = world.local_pipe()
        else:
            emitter, receiver = world.mp_pipe(sensor_spec.transport)
        sensor_pipes.append((sensor_spec, emitter, receiver))

    sensor_loops = [
        (sensor_loop_gen, (emitter, sensor))  # ← Fixed variable name
        for sensor, emitter, _ in sensor_pipes
    ]

    sensor_receivers = [(sensor, receiver) for sensor, _, receiver in sensor_pipes]
    controller_loops = [
        (controller_loop_gen, (sensor_receivers,))  # ← Use sensor_receivers, not receivers
    ]

    if PARALLELISM_TYPE == ParallelismType.LOCAL:
        cooperative_loops = sensor_loops + controller_loops
        bg_loops = []   # No background processes
    elif PARALLELISM_TYPE == ParallelismType.MULTIPROCESS:
        cooperative_loops = controller_loops
        bg_loops = sensor_loops
    else:
        raise ValueError(" Wrong parallelism type")


    # START simulation - run sensor robotics_control_loop in background and controllers loop cooperatively
    world.start(cooperative_loops, bg_loops)





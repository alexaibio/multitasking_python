"""
Further development of cooperative scheduling loop.
Here it has been added:
  - running sensors in a separate process
  - interprocess shared memory and queue transport

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

from shared_memory import SMCompliant, NumpySMAdapter


T = TypeVar("T")


class TransportMode(IntEnum):
    QUEUE = 1
    SHARED_MEMORY = 2


class ParallelismType(IntEnum):
    LOCAL = 0
    MULTIPROCESS = 2


@dataclass
class Message(Generic[T]):
    data: T
    ts: float
    updated: bool = True    # is data new since the last read? Set by receiver



@dataclass
class Sleep:
    seconds: float


class Clock:
    def now_ns(self) -> int:
        return time.time_ns()

### Define sensors
class Sensor(ABC):
    def __init__(self, sensor_id: str, transport: TransportMode, interval: float):
        self.sensor_id = sensor_id
        self.transport = transport
        self.interval = interval

    @abstractmethod
    def read(self) -> Any:
        """Read sensor value."""
        pass


class TemperatureSensor(Sensor):
    def __init__(self, transport: TransportMode, interval: float):
        super().__init__(sensor_id="temp_sensor", transport=transport, interval=interval)
        self.unit = "°C"

    def read(self) -> float:
        return round(20 + random.uniform(-2, 5), 2)


class CloudinessSensor(Sensor):
    def __init__(self, transport, interval: float):
        super().__init__(sensor_id="cloudy_sensor", transport=transport, interval=interval)

    def read(self) -> str:
        return random.choice(["Clear", "Partly Cloudy", "Cloudy", "Rain"])


class CameraSensor(Sensor):
    def __init__(self, transport: TransportMode, interval: float):
        super().__init__(sensor_id="camera", transport=transport, interval=interval)
        self.width = 200
        self.height = 320
        self.unit = "frame"

    def read(self) -> np.ndarray:
        frame = np.random.randint(0, 256, (self.height, self.width, 3), dtype=np.uint8)
        return frame



######### Emitters / Receivers
class SignalEmitter(Generic[T]):
    def emit(self, data: T) -> bool:
        """Add data to a queue as a Message"""
        ...

class SignalReceiver(Generic[T]):
    def read(self) -> Message[T] | None:
        """Returns next message, otherwise last value. None if nothing was read yet."""
        ...


class LocalQueueEmitter(SignalEmitter[T]):
    def __init__(self, queue: deque, clock: Clock):
        self.queue = queue
        self.clock = clock

    def emit(self, data: T):
        msg = Message(data=data, ts=self.clock.now_ns())       # TODO: better get it from World as parameter
        self.queue.append(msg)

        print_friendly_data = msg.data if not isinstance(msg.data, np.ndarray) else msg.data[1][0,0].tolist()
        print(f"[Local Emitter] Emitted |{print_friendly_data}| at {msg.ts:.2f}\n")
        return True


class LocalQueueReceiver(SignalReceiver[T]):
    def __init__(self, queue: deque):
        self.queue = queue
        self.last_msg = None    # if no new data, emit the last message

    def read(self) -> Message[T] | None:
        if self.queue:
            self.last_msg = self.queue.popleft()
        return self.last_msg


# Multiprocess emitter/receiver using multiprocessing.Queue
class MultiprocessEmitter(SignalEmitter[T]):
    """Emitter for inter-process communication."""

    def __init__(self,
                 transport: TransportMode,
                 queue: mp.Queue,               # use for QUEUE transport
                 clock: Clock,
                 lock: mp.Lock = None,
                 ts_value: mp.Value = None,     # time of emitting fresh value
                 up_value: mp.Value = None,     # for SM: flag of fresh SM block
                 sm_queue: mp.Queue = None):    # for SM: SM metadata queue
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
        """
        Send via regular queue.
        Not too efficient for camera data, every frame must go via queue
        """
        msg = Message(data=data, ts=ts)
        try:
            self.queue.put_nowait(msg)
            return True
        except Full:
            try:
                self.queue.get_nowait()     # drop the oldest of queue is full
                self.queue.put_nowait(msg)
                return True
            except (Empty, Full):
                return False

    def _emit_shared_memory(self, data: SMCompliant, ts: float) -> bool:
        """Send via shared memory."""
        assert isinstance(data, SMCompliant), f"SHARED_MEMORY mode requires SMCompliant data, got {type(data)}"
        buf_size = data.buf_size()

        # First time: create buffer and send metadata
        if self._sm is None:
            self._expected_buf_size = buf_size
            self._sm = shared_memory.SharedMemory(create=True, size=buf_size)
            sm_metadata = (
                self._sm.name,
                buf_size,
                type(data),
                data.instantiation_params())
            self.sm_queue.put(sm_metadata)      # send SM metadata
            print(f"      [Emitter] Created SM buffer and sent its metadata: {self._sm.name}, size={buf_size}")

        assert buf_size == self._expected_buf_size, f"Buffer size changed: {buf_size} != {self._expected_buf_size}"

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
            data_sm = NumpySMAdapter.lazy_init(data)
            self._emit_shared_memory(data_sm, ts)
        else:
            self._emit_queue(data, ts)
        print(f"[Emitter] Data is emitted")

    def close(self) -> None:
        """Clean up shared memory."""
        if self._sm is not None:
            try:
                self._sm.close()    # stop using it
                self._sm.unlink()   # tell OS to delete the block
            except:
                pass


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
        self._out_value: SMCompliant | None = None    # buffer that will hold the latest data read from shared memory
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
            # data_type: class that interprets the bytes, NumpySMAdapter
            # instantiation_params: size of numpy array
            sm_name, buf_size, data_type, instantiation_params = self.sm_queue.get_nowait()
        except Empty:
            return False

        # Attach Receiver to existing shared memory
        self._sm = shared_memory.SharedMemory(name=sm_name)

        # The creator process is responsible for unlinking it, not the receiver
        try:
            resource_tracker.unregister(self._sm._name, 'shared_memory')
        except:
            pass

        # in case buffer is slightly larger than requested;
        self._readonly_buffer = self._sm.buf.toreadonly()[:buf_size]
        self._out_value = data_type(*instantiation_params)

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
        else:
            return self._read_queue()

    def close(self) -> None:
        if self._readonly_buffer is not None:
            self._readonly_buffer.release()
        if self._sm is not None:
            self._sm.close()


# ---------------------------------------------------------------------
# Control Loops
# ---------------------------------------------------------------------

# helper function to check stop condition
def should_stop(stop_event) -> bool:
    if stop_event is None:
        return False
    elif hasattr(stop_event, 'is_set'):
        return stop_event.is_set()
    else:
        return stop_event


def sensor_loop_gen_fn(stop_event: mp.Event, emitter: SignalEmitter, sensor: Sensor):
    """
    Generator (NOTE: it cannot be sent to a thread because of serialization issue, needs a wrapper!)
     - permanently read one sensor and emit its reading into queue or shared memory
     - return a command to be executed in main cooperative loop (Sleep)
    """
    while not should_stop(stop_event):
        reading = sensor.read()
        emitter.emit(reading)
        yield Sleep(sensor.interval)    # Give control back to the world

    print("[Sensor] loop ended.", flush=True)


def controller_loop_gen_fn(stop_event: mp.Event,
                           receivers: List[Tuple[Sensor, SignalReceiver]]):
    """Controller loop - reads from sensors and acts."""
    while not should_stop(stop_event):
        # read all sensors once
        for sensor, receiver in receivers:
            msg = receiver.read()
            if msg:
                value = msg.data

                # Extract from adapter if Shared Memory
                if isinstance(value, NumpySMAdapter):
                    value = value.array

                display_value = value if not isinstance(value, np.ndarray) else f"array[0,0]={value[0, 0]}"

                status = "FRESH" if msg.updated else "STALE"
                print(f"[Controller] {sensor.sensor_id} received: {display_value} [{status}]", end='')

                if msg.updated:
                    print(f"    → Action required based on {sensor.sensor_id} reading")

        yield Sleep(5)      # Give control back to the world
    print("[Controller] loop ended.")


def _bg_wrapper_loop(sensor_loop_fn, stop_event, *args):
    """
     Needed because we cannot run sensor_loop_gen in a separate process (serialization issue)
    """
    generator_fn = sensor_loop_fn(stop_event, *args)
    try:
        for command in generator_fn:
            if isinstance(command, Sleep):
                stop_event.wait(command.seconds)    #  until the event is set, or timeout seconds have passed
            else:
                raise ValueError(f'Unknown command: {command}')
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[Background] Stopping {sensor_loop_fn.__name__} with {args[1].sensor_id} inside")


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

    def start(self, fg_loops, bg_loops):
        print(f"[world] is starting")

        ### Start background interprocess loops
        for background_fn, args in bg_loops:
            pr = mp.Process(target=_bg_wrapper_loop, args=(background_fn, self._stop_event, *args))
            pr.start()
            self.background_processes.append(pr)
            print(f"[World] Started background process for {args[1].sensor_id}, {args[1].transport.name}")

        #### Run main loop (cooperative scheduling - coroutines) inside the main process
        try:                    # handle KeyboardInterrupt
            # create all generators
            generators = [
                cooperative_fn(self._stop_event, *args)
                for cooperative_fn, args in fg_loops
            ]
            while not self._stop_event.is_set():
                # allows handling multiple controllers (whole system)
                for gen in generators:
                    try:        # handle StopIteration when generator finishes
                        command = next(gen)
                        # execute command: might also be Stor, Log - only control flow commands not actuators
                        if isinstance(command, Sleep):
                            time.sleep(command.seconds)
                        else:
                            raise ValueError(f" Wrong command {command}")
                    except StopIteration:
                        print(f'...Control loop generator  {gen} stopped')
                        continue
        except KeyboardInterrupt:
            pass
        finally:
            print("[World] Stopping... sending stop_event")
            self._stop_event.set()

            # Cleanup
            for emitter, receiver in self._cleanup_resources:
                emitter.close()     # clean up shared memory
                receiver.close()
            print("  ... Shared memory released by all emitters and receivers")

            print("  ... joining each bg process to let stop_event make effect")
            for pr in self.background_processes:
                pr.join()


# ---------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------
if __name__ == "__main__":
    PARALLELISM_TYPE = ParallelismType.MULTIPROCESS

    sensors: list[Sensor] = [
        TemperatureSensor(transport=TransportMode.QUEUE, interval=1.0),
        CloudinessSensor(transport=TransportMode.QUEUE, interval=5.0),
        #CameraSensor(transport=TransportMode.QUEUE, interval=2.0)
        CameraSensor(transport=TransportMode.SHARED_MEMORY, interval=2.0)
    ]

    # Ensure SM only used in MULTIPROCESS mode
    if PARALLELISM_TYPE == ParallelismType.LOCAL and (
            wrong_s := [s for s in sensors if s.transport==TransportMode.SHARED_MEMORY]):
        raise ValueError(f"SHARED_MEMORY transport not allowed in LOCAL mode for {[s.sensor_id for s in wrong_s]}")



    #### World Simulation
    world = World()         # CLOCK CREATED INSIDE WORLD to synchronize all sensors

    # create all pipes between two sensors (emitter -> receiver)
    pipes: list[Tuple[Sensor, SignalEmitter, SignalReceiver]] = []
    for sensor in sensors:
        if PARALLELISM_TYPE == ParallelismType.LOCAL:
            emitter, receiver = world.local_pipe()
        else:
            emitter, receiver = world.mp_pipe(sensor.transport)
        pipes.append((sensor, emitter, receiver))

    # list of nessesary emitting loops
    emitting_loops = [
        (sensor_loop_gen_fn, (emitter, sensor))
        for sensor, emitter, _ in pipes
    ]

    # just on controller cooperative loop with all receivers in it
    receivers = [(sensor, receiver) for sensor, _, receiver in pipes]
    controller_loop = [
        (controller_loop_gen_fn, (receivers,))
    ]

    # decide what to run in background or foreground
    if PARALLELISM_TYPE == ParallelismType.LOCAL:
        cooperative_loops = emitting_loops + controller_loop
        bg_loops = []   # No background processes
    elif PARALLELISM_TYPE == ParallelismType.MULTIPROCESS:
        cooperative_loops = controller_loop
        bg_loops = emitting_loops
    else:
        raise ValueError(" Wrong parallelism type")


    # START simulation - run sensor robotics_control_loop in background and controllers loop cooperatively
    world.start(cooperative_loops, bg_loops)





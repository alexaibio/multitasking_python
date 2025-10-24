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
import multiprocessing as mp
from collections import deque
from enum import IntEnum
from dataclasses import dataclass
from typing import Generic, TypeVar, Sequence, List, Tuple, Any, Callable
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
    interval: float = 1.0
    unit: str | None = None

### Define how each sensor gets its reading
def read_temp() -> float:
    return round(20 + random.uniform(-2, 5), 2)

def read_cloudiness() -> str:
    return random.choice(["Clear", "Partly Cloudy", "Cloudy", "Rain"])

def read_pressure() -> float:
    return round(1000 + random.uniform(-10, 10), 2)

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



#### Interfaces (Typing classes)
class SignalEmitter(Generic[T]):
    def emit(self, data: T) -> bool:
        """Add data to a queue as a Message"""
        ...

class SignalReceiver(Generic[T]):
    def read(self) -> Message[T] | None:
        """Returns next message, otherwise last value. None if nothing was read yet."""
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


# Multiprocess emitter/receiver using multiprocessing.Queue
class MultiprocessEmitter(SignalEmitter[T]):
    """Emitter for inter-process communication."""
    def __init__(self, queue: mp.Queue, clock: Clock):
        self.queue = queue
        self.clock = clock

    def emit(self, data: T) -> None:
        msg = Message(data=data, ts=self.clock.now())

        # PUT() might block - does not return until some space becomes available.
        # use put_nowait() for non-blocking behaviour
        self.queue.put(msg)
        print(f"[MP Emitter] Emitted {data} at {msg.ts:.2f}\n")


class MultiprocessReceiver(SignalReceiver[T]):
    """Receiver for inter-process communication."""
    def __init__(self, queue: mp.Queue):
        self.queue = queue
        self.last_msg: Message[T] | None = None

    def read(self) -> Message[T] | None:
        try:
            self.last_msg = self.queue.get_nowait()
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
# Control Loop primitives
# ---------------------------------------------------------------------


def sensor_loop_gen(stop_event: mp.Event, emitter: SignalEmitter, sensor: SensorSpec):
    """
    Generator (cannot be sent to a thread)
     - permanently read one sensor and emit its reading to queue
     - return a command to be executed in main cooperative loop
    """
    while not stop_event.is_set():
        reading = sensor.read_fn()
        emitter.emit((sensor.id, reading))  # emits labeled reading as tuple
        yield Sleep(1.0)                    # loop voluntarily pauses, World gets control back


def controller_loop_gen(stop_event: mp.Event, receivers: List[SignalReceiver]):
    """
    Generator: for now single controller handles all sensors
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
     - Ready to be sent to bg thread, run until stop_event is set.
     - Execute command returned from generator
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


    def start(self, controller_loops, bg_loops):
        print("[world] is starting")
        # Start keypress watcher
        threading.Thread(target=self._keypress_watcher, daemon=True).start()

        ### Start background loops - true parallelism, in separate threads/processes.
        for sensor_fn, args in bg_loops:
            if PARALLELISM_TYPE == ParallelismType.PROCESS:     # run but Process
                pr = mp.Process(target=_bg_wrapper_loop, args=(sensor_fn, self._stop_event, *args))
                pr.start()
                self.background_processes.append(pr)
                print(f"[World] Started background process for {sensor_fn.__name__}")
            else:                   # run by THREAD
                thr = threading.Thread(target=_bg_wrapper_loop, args=(sensor_fn, self._stop_event, *args), daemon=True)
                thr.start()
                self.background_processes.append(thr)
                print(f"[World] Started background thread for {sensor_fn.__name__}")

        #### Run main loop (cooperative scheduling -  coroutines) inside the main thread
        try:                    # handle KeyboardInterrupt
            while not self._stop_event.is_set():
                # allows handling multiple controllers (whole system)
                for sensor_fn, args in controller_loops:
                    try:        # handle StopIteration when generator finishes
                        command = next(sensor_fn(self._stop_event, *args)) # generator
                        # execute command: might also be Stor, Log - only control flow commands not actuators
                        if isinstance(command, Sleep):
                            time.sleep(command.seconds)
                        else:
                            raise ValueError(f" Wrong command {command}")
                    except StopIteration:
                        print(f'...Control loop generator  {sensor_fn} stopped')
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
    # Select execution mode here:
    PARALLELISM_TYPE = ParallelismType.PROCESS      # thread / process
    TRANSPORT_MODE = TransportMode.QUEUE            # queue / shared_memory

    # Define sensors
    sensor_specs: list[SensorSpec] = [
        SensorSpec(id="temp_sensor", dtype=float, read_fn=read_temp, interval=1.0, unit="°C"),
        SensorSpec(id="cloudy_sensor", dtype=str, read_fn=read_cloudiness, interval=2.0),
        SensorSpec(id="pressure_sensor", dtype=float, read_fn=read_pressure, interval=1.5, unit="hPa"),
    ]

    #### World Simulation
    world = World()         # CLOCK CREATED INSIDE WORLD to synchronize all sensors

    # create (emitter, receiver) pipe for sensors
    sensor_pipes: list[Tuple[SensorSpec, SignalEmitter, SignalReceiver]] = []

    emitter: MultiprocessEmitter
    receiver: MultiprocessReceiver
    for sensor_spec in sensor_specs:
        if PARALLELISM_TYPE == ParallelismType.PROCESS: # Process mode
            emitter, receiver = world.new_mp_pipe()
        else:                                    # THREAD mode: Sensor & controller live in the same process
            emitter, receiver = world.new_local_pipe()

        sensor_pipes.append((sensor_spec, emitter, receiver))

    # create background loops list automatically
    bg_loops = [
        (sensor_loop_gen, (emitter, spec)) for spec, emitter, _ in sensor_pipes
    ]

    # controller takes all receivers dynamically
    receivers = [r for _, _, r in sensor_pipes]
    controller_loops = [
        (controller_loop_gen, (receivers,))
    ]

    # START simulation - run sensor robotics_control_loop in background and controllers loop cooperatively
    world.start(controller_loops, bg_loops)

    # TODO: to stop the world it is possioble to run another process with stop event




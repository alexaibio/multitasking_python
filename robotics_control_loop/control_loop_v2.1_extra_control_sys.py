"""
Decouple emitter and reveiver from SensorControlSystem
  SensorSystem(..., emitter=LocalQueueEmitter(...))
  ControllerSystem(..., receivers=[LocalQueueReceiver(...), ...])

that previous implementation hard-codes the communication details into the systems,
You can’t dynamically “connect” systems without rewriting code.


"""


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
from typing import Generic, TypeVar, Sequence, List, Tuple, Any, Callable, Iterator, Iterable
from abc import ABC, abstractmethod
import select

T = TypeVar("T")
U = TypeVar("U")


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
    A substitution for senser_loop and controller_loop
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
# Control Systems
# ---------------------------------------------------------------------


class ControlSystemEmitter(SignalEmitter[T]):
    """Emitter adaptor that keeps track of its owning control system."""

    def __init__(self, owner: ControlSystem):
        self._owner = owner
        self._internal: list[SignalEmitter[T]] = []

    @property
    def owner(self) -> ControlSystem:
        return self._owner

    @property
    def num_bound(self) -> int:
        return len(self._internal)

    def _bind(self, emitter: SignalEmitter[T]):
        self._internal.append(emitter)

    def emit(self, data: T, ts: int = -1) -> bool:
        for emitter in self._internal:
            emitter.emit(data, ts)
        # TODO: Remove bool as return type from all Emitters
        return True


class ControlSystemReceiver(SignalReceiver[T]):
    """Receiver adaptor bound to a single upstream signal on behalf of a system."""

    def __init__(self, owner: ControlSystem, maxsize: int | None = None):
        self._owner = owner
        self._internal: SignalReceiver[T] | None = None
        self._maxsize = maxsize

    @property
    def maxsize(self) -> int | None:
        return self._maxsize

    @property
    def owner(self) -> ControlSystem:
        return self._owner

    def _bind(self, receiver: SignalReceiver[T]):
        assert self._internal is None, 'Receiver can be connected only to one Emitter'
        self._internal = receiver

    def read(self) -> Message[T] | None:
        return self._internal.read() if self._internal is not None else None


class FakeEmitter(ControlSystemEmitter[T]):
    """Placeholder emitter for optional outputs.

    Used for duck typing compatibility when control systems have different interfaces.
    World.connect ignores connections involving FakeEmitter, preventing signal flow.
    """

    def emit(self, data: T, ts: int = -1) -> bool:
        raise RuntimeError('FakeEmitter.emit() is not supposed to be called')

    def _bind(self, emitter: SignalEmitter[T]):
        raise RuntimeError('FakeEmitter._bind() is not supposed to be called')


class FakeReceiver(ControlSystemReceiver[T]):
    """Placeholder receiver for optional inputs.

    Used for duck typing compatibility when control systems have different interfaces.
    World.connect ignores connections involving FakeReceiver, preventing signal flow.
    """

    def read(self) -> Message[T] | None:
        raise RuntimeError('FakeReceiver.read() is not supposed to be called')

    def _bind(self, receiver: SignalReceiver[T]):
        raise RuntimeError('FakeReceiver._bind() is not supposed to be called')


class ReceiverDict(dict[str, ControlSystemReceiver[U]]):
    """Dictionary that lazily allocates receivers owned by a control system.

    Pass fake=True for all fake receivers, or fake={'key1', 'key2'} for specific keys.
    """

    def __init__(self, owner: ControlSystem, fake: bool | Iterable[str] = False):
        super().__init__()
        self._owner = owner
        self._fake = set(fake) if isinstance(fake, Iterable) else set()
        self._all_fake = fake is True

    def __missing__(self, key: str) -> ControlSystemReceiver[U]:
        fake = self._all_fake or key in self._fake
        receiver = FakeReceiver(self._owner) if fake else ControlSystemReceiver(self._owner)
        self[key] = receiver
        return receiver


class EmitterDict(dict[str, ControlSystemEmitter[U]]):
    """Dictionary that lazily allocates emitters owned by a control system.

    Pass fake=True for all fake emitters, or fake={'key1', 'key2'} for specific keys.
    """

    def __init__(self, owner: ControlSystem, fake: bool | Iterable[str] = False):
        super().__init__()
        self._owner = owner
        self._fake = set(fake) if isinstance(fake, Iterable) else set()
        self._all_fake = fake is True

    def __missing__(self, key: str) -> ControlSystemEmitter[U]:
        # extend dictionary: auto-create emitters when accessed
        # for example by sensor.emitters["data"] - create a new ControlSystemEmitter(owner=sensor)
        fake = self._all_fake or key in self._fake
        emitter = FakeEmitter(self._owner) if fake else ControlSystemEmitter(self._owner)
        self[key] = emitter
        return emitter


#####  SensorSystem and ControllerSystem implementations
########################################################

class SensorSystem(ControlSystem):
    """Sensor runs in background and periodically emits readings."""
    def __init__(self, sensor: SensorSpec):
        self.sensor = sensor
        self.emitters = EmitterDict(self)
        self.emitters["data"]           # create port lazily

    def run(self, should_stop: mp.Event, clock: Clock) -> Iterator[Sleep]:
        emitter = self.emitters["data"]
        while not should_stop.is_set():
            reading = self.sensor.read_fn()
            emitter.emit((self.sensor.id, reading))
            yield Sleep(self.sensor.interval)


class ControllerSystem(ControlSystem):
    def __init__(self):
        self.receivers = ReceiverDict(self)

    def run(self, should_stop: mp.Event, clock: Clock) -> Iterator[Sleep]:
        while not should_stop.is_set():
            for name, receiver in self.receivers.items():
                msg = receiver.read()
                if msg:
                    sensor_name, value = msg.data
                    action = "COOL" if isinstance(value, (int, float)) and value > 25 else "HEAT"
                    print(f"[Controller] {name}: {value} → {action}")
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
    # Select execution mode here:
    PARALLELISM_TYPE = ParallelismType.PROCESS
    TRANSPORT_MODE = TransportMode.QUEUE

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

    # create background loops list automatically using SensorSystem instances
    bg_loops: List[ControlSystem] = [
        SensorSystem(spec, emitter) for spec, emitter, _ in sensor_pipes
    ]  # CHANGED: use SensorSystem objects

    # controller takes all receivers dynamically
    receivers = [r for _, _, r in sensor_pipes]
    controller_loops: List[ControlSystem] = [
        ControllerSystem(receivers)
    ]  # CHANGED: use ControllerSystem object

    # START simulation - run sensor robotics_control_loop in background and controllers loop cooperatively
    world.start(controller_loops, bg_loops)

    # TODO: to stop the world it is possible to run another process with stop event




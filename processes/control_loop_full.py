"""
 - LocalQueueEmitter   -  Uses an in-memory `deque` → communication within the same process
 - MultiprocessEmitter -  Uses `multiprocessing.Queue` → communication between processes
 - BroadcastEmitter    -  Sends each message to several emitters at once
 - TransportMode       -  Enum that decides which transport to use (queue or shared memory)

"""
import sys
import time
import threading
import multiprocessing as mp
from collections import deque
from enum import IntEnum
from dataclasses import dataclass
from typing import Generic, TypeVar, Sequence, List, Tuple, Any
import select

T = TypeVar("T")


class TransportMode(IntEnum):
    UNDECIDED = 0
    QUEUE = 1
    SHARED_MEMORY = 2  # Not really used here, just to mirror your structure


class ParallelismType(IntEnum):
    THREAD = 1
    PROCESS = 2

# Select execution mode here:
PARALLELISM_TYPE = ParallelismType.PROCESS  # (change to PROCESS as needed)


@dataclass
class Message(Generic[T]):
    data: T
    ts: float


class Clock:
    def now(self) -> float:
        return time.time()

    def now_ns(self) -> int:
        """Current timestamp in nanoseconds."""
        return time.time_ns()



#### Typing classes
class SignalEmitter(Generic[T]):
    def emit(self, data: T) -> None:
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
        msg = Message(data=data, ts=self.clock.now())
        self.queue.append(msg)
        print(f"[Local Emitter] Emitted |{data}| at {msg.ts:.2f}\n")


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
        self.queue.put(msg)         # different from local dequeue
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
@dataclass
class Sleep:
    seconds: float


def sensor_loop(stop_event: mp.Event, emitter, sensor_id: str):
    """Generator: Simulate sensor loop with optional ID."""
    while not stop_event.is_set():
        reading = round(20 + 5 * (time.time() % 10), 2)
        emitter.emit((sensor_id, reading))  # <<< emits labeled reading
        yield Sleep(2.0)                    # loop voluntarily pauses, World gets control back


def controller1_loop(stop_event: mp.Event, receivers: List[SignalReceiver]):
    """
    Generator: for now single controller handles all sensors
    """
    while not stop_event.is_set():
        for i, r in enumerate(receivers):  # iterate all sensors
            msg = r.read()
            if msg:
                sensor_id, value = msg.data
                action = "COOL" if value > 25 else "HEAT"
                print(f"  [Controller] Sensor [{sensor_id}] reading: {value:.2f}, Action: {action}")

        yield Sleep(5)      # loop voluntarily pauses, World gets control back



def _bg_wrapper(control_loop_fn, stop_event, *args):
    """Run a ControlLoop continuously until stop_event is set."""
    loop = control_loop_fn(stop_event, *args)
    try:
        for command in loop:
            if isinstance(command, Sleep):
                time.sleep(command.seconds)
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[Background] Stopping {control_loop_fn.__name__}")


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

    def create_local_pipe(self):
        """ Create data queue and assign it to both emitter and receiver """
        q = deque(maxlen=5)
        emitter = LocalQueueEmitter(q, self.clock)
        receiver = LocalQueueReceiver(q)
        return emitter, receiver

    # multiprocessing pipe creator
    def create_mp_pipe(self):
        q = mp.Queue(maxsize=5)
        emitter = MultiprocessEmitter(q, self.clock)
        receiver = MultiprocessReceiver(q)
        return emitter, receiver

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()


    def start(self, controller_loops, background_loops):
        # Start keypress watcher
        threading.Thread(target=self._keypress_watcher, daemon=True).start()

        # Start background loops - independently, in separate threads or processes.
        for fn, args in background_loops:
            if PARALLELISM_TYPE == ParallelismType.PROCESS:
                p = mp.Process(target=_bg_wrapper, args=(fn, self._stop_event, *args))
                p.start()
                self.background_processes.append(p)
                print(f"[World] Started background process for {fn.__name__}")
            else:  # THREAD
                t = threading.Thread(target=_bg_wrapper, args=(fn, self._stop_event, *args), daemon=True)
                t.start()
                self.background_processes.append(t)
                print(f"[World] Started background thread for {fn.__name__}")

        # Run main loops cooperatively - right here, inside the main thread.
        try:
            while not self._stop_event.is_set():
                for fn, args in controller_loops:
                    loop = fn(self._stop_event, *args)
                    try:
                        command = next(loop)        # raise StopIteration when generator finishes
                        if isinstance(command, Sleep):
                            time.sleep(command.seconds)
                        else:
                            print('Command is not recognized!')
                    except StopIteration:
                        print('Generator stopped by stop_event')
                        continue
        except KeyboardInterrupt:
            pass
        finally:
            print("[World] Stopping...")
            self._stop_event.set()
            for p in self.background_processes:
                p.join()


# ---------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------
if __name__ == "__main__":
    world = World()         # CLOCK CREATED INSIDE WORLD to synchronize all sensors

    # create (emitter, receiver) pipe for sensors
    sensor_specs: dict[str, type] = {
        "Temperature": float,
        "Cloud": str,
    }
    sensors: list[Tuple[str, SignalEmitter, SignalReceiver]] = []

    for sid, dtype in sensor_specs.items():
        if PARALLELISM_TYPE == ParallelismType.PROCESS:
            if dtype is float:
                emitter: MultiprocessEmitter[float]
                receiver: MultiprocessReceiver[float]
            elif dtype is str:
                emitter: MultiprocessEmitter[str]
                receiver: MultiprocessReceiver[str]
            else:
                raise TypeError(f"Unsupported sensor type: {dtype}")
            emitter, receiver = world.create_mp_pipe()
        else:  # THREAD mode
            if dtype is float:
                emitter: LocalQueueEmitter[float]
                receiver: LocalQueueReceiver[float]
            elif dtype is str:
                emitter: LocalQueueEmitter[str]
                receiver: LocalQueueReceiver[str]
            else:
                raise TypeError(f"Unsupported sensor type: {dtype}")
            emitter, receiver = world.create_local_pipe()

        sensors.append((sid, emitter, receiver))

    # create background loops list automatically
    bg_loops = [(sensor_loop, (emitter, sid)) for sid, emitter, _ in sensors]

    # controller takes all receivers dynamically
    receivers = [r for _, _, r in sensors]
    controller_loops = [
        (controller1_loop, (receivers,))
    ]

    # START simulation - run sensor processes in background and controllers loop cooperatively
    world.start(controller_loops, bg_loops)



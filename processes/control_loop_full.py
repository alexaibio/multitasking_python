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
from typing import Generic, TypeVar, Sequence, List
import select

T = TypeVar("T")


class TransportMode(IntEnum):
    UNDECIDED = 0
    QUEUE = 1
    SHARED_MEMORY = 2  # Not really used here, just to mirror your structure


class ParallelismType(IntEnum):  # <<< ADDED
    THREAD = 1
    PROCESS = 2

# Select execution mode here:
PARALLELISM_TYPE = ParallelismType.PROCESS  # <<< ADDED  (change to PROCESS as needed)


@dataclass
class Message(Generic[T]):
    data: any
    ts: float


#### Typing classes
class SignalEmitter(Generic[T]):
    """Base class for type hinting."""
    def emit(self, data: T) -> None:
        ...

class SignalReceiver(Generic[T]):
    def read(self) -> Message[T] | None:
        """Returns next message, otherwise last value. None if nothing was read yet."""
        pass


####### Emitters / Receivers
class LocalQueueEmitter(SignalEmitter[T]):
    def __init__(self, queue: deque):
        self.queue = queue

    def emit(self, data):
        msg = Message(data=data, ts=time.time())
        self.queue.append(msg)
        print(f"[Emitter] Emitted {data} at {msg.ts:.2f}")


class LocalQueueReceiver(SignalReceiver[T]):
    def __init__(self, queue: deque):
        self.queue = queue
        self.last_msg = None

    def read(self) -> Message[T] | None:
        if self.queue:
            self.last_msg = self.queue.popleft()
        return self.last_msg


# Multiprocess emitter/receiver using multiprocessing.Queue
class MultiprocessEmitter(SignalEmitter[T]):
    """Emitter for inter-process communication."""
    def __init__(self, queue: mp.Queue):
        self.queue = queue

    def emit(self, data: T) -> None:
        msg = Message(data=data, ts=time.time())
        self.queue.put(msg)
        print(f"[MP Emitter] Emitted {data} at {msg.ts:.2f}")


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


def sensor_loop(stop_event, emitter, sensor_id="A"):  # <<< ADDED optional sensor_id
    """Simulated sensor loop with optional ID."""
    while not stop_event.is_set():
        reading = round(20 + 5 * (time.time() % 10), 2)
        emitter.emit((sensor_id, reading))  # <<< emits labeled reading
        yield Sleep(2.0)                    # Sleep 5s between measurements


def controller_loop(stop_event, receivers: List[SignalReceiver]):  # <<< MODIFIED to list
    """Controller automatically reads from all sensors."""
    while not stop_event.is_set():
        for i, r in enumerate(receivers):  # <<< iterate all sensors
            msg = r.read()
            if msg:
                sensor_id, value = msg.data
                action = "COOL" if value > 25 else "HEAT"
                print(f"[Controller] Sensor {sensor_id}: {value:.2f}, Action: {action}")
        yield Sleep(5)



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
# The “World” — orchestrating control loops
# ---------------------------------------------------------------------
class World:
    def __init__(self):
        self._stop_event = mp.Event()
        self.bg_threads_or_processes = []

    def _keypress_watcher(self):
        print("[World] Press any key to stop simulation...")
        while not self._stop_event.is_set():
            if select.select([sys.stdin], [], [], 0.1)[0]:
                sys.stdin.read(1)
                print("\n[World] Key pressed. Stopping...")
                self._stop_event.set()

    def local_pipe(self):
        q = deque(maxlen=5)
        return LocalQueueEmitter(q), LocalQueueReceiver(q)

    # multiprocessing pipe creator
    def mp_pipe(self):
        q = mp.Queue(maxsize=5)
        return MultiprocessEmitter(q), MultiprocessReceiver(q)

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()


    def start(self, main_loops, background_loops):
        # keypress watcher
        threading.Thread(target=self._keypress_watcher, daemon=True).start()


        # Start background loops
        for fn, args in background_loops:
            if PARALLELISM_TYPE == ParallelismType.PROCESS:
                p = mp.Process(target=_bg_wrapper, args=(fn, self._stop_event, *args))
                p.start()
                self.bg_threads_or_processes.append(p)
                print(f"[World] Started background process for {fn.__name__}")
            else:  # THREAD
                t = threading.Thread(target=_bg_wrapper, args=(fn, self._stop_event, *args), daemon=True)
                t.start()
                self.bg_threads_or_processes.append(t)
                print(f"[World] Started background thread for {fn.__name__}")

        # Run main loops cooperatively
        try:
            while not self._stop_event.is_set():
                for fn, args in main_loops:
                    loop = fn(self._stop_event, *args)
                    try:
                        command = next(loop)
                        if isinstance(command, Sleep):
                            time.sleep(command.seconds)
                    except StopIteration:
                        continue
        except KeyboardInterrupt:
            pass
        finally:
            print("[World] Stopping...")
            self._stop_event.set()
            for p in self.bg_threads_or_processes:
                p.join()

# ---------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------
if __name__ == "__main__":
    world = World()

    # create multiple pipes for sensors
    sensors = []
    for sid in ["Sensor A", "Sensor B", "Sensor C"]:
        if PARALLELISM_TYPE == ParallelismType.PROCESS:      # <<< ADDED
            emitter, receiver = world.mp_pipe()
        else:  # THREAD mode
            emitter, receiver = world.local_pipe()
        sensors.append((sid, emitter, receiver))

    # create background loops list automatically
    bg_loops = [(sensor_loop, (emitter, sid)) for sid, emitter, _ in sensors]

    # controller takes all receivers dynamically
    receivers = [r for _, _, r in sensors]
    main_loops = [(controller_loop, (receivers,))]

    # START simulation - all processes
    world.start(main_loops, bg_loops)



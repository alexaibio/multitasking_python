"""
 - LocalQueueEmitter   -  Uses an in-memory `deque` → communication within the same process
 - MultiprocessEmitter -  Uses `multiprocessing.Queue` → communication between processes
 - BroadcastEmitter    -  Sends each message to several emitters at once
 - TransportMode       -  Enum that decides which transport to use (queue or shared memory)

"""

import time
import multiprocessing as mp
from collections import deque
from enum import IntEnum
from dataclasses import dataclass
from typing import Generic, TypeVar, Sequence

T = TypeVar("T")


@dataclass
class Message(Generic[T]):
    data: any
    ts: float


class TransportMode(IntEnum):
    UNDECIDED = 0
    QUEUE = 1
    SHARED_MEMORY = 2  # Not really used here, just to mirror your structure


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

def sensor_loop(stop_event, emitter):
    """Simulated ControlLoop: reads sensor and emits measurements."""
    while not stop_event.is_set():
        reading = round(20 + 5 * time.time() % 10, 2)  # Fake temperature
        emitter.emit(reading)
        yield Sleep(1.0)  # Sleep 1s between measurements


def controller_loop(stop_event, receiver):
    """Simulated ControlLoop: reads from sensor and decides action."""
    while not stop_event.is_set():
        msg = receiver.read()
        if msg:
            action = "COOL" if msg.data > 25 else "HEAT"
            print(f"[Controller] Received {msg.data:.2f}, Action: {action}")
        yield Sleep(0.5)



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
        self.stop_event = mp.Event()
        self.bg_processes = []

    def local_pipe(self):
        q = deque(maxlen=5)
        return LocalQueueEmitter(q), LocalQueueReceiver(q)

    # NEW: Added multiprocessing pipe creator
    def mp_pipe(self):
        q = mp.Queue(maxsize=5)
        return MultiprocessEmitter(q), MultiprocessReceiver(q)

    def start(self, main_loops, background_loops):
        # Start background control loops as separate processes
        for fn, args in background_loops:
            p = mp.Process(target=_bg_wrapper, args=(fn, self.stop_event, *args))
            p.start()
            self.bg_processes.append(p)
            print(f"[World] Started background process for {fn.__name__}")

        # Run main loops cooperatively
        try:
            while not self.stop_event.is_set():
                for fn, args in main_loops:
                    loop = fn(self.stop_event, *args)
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
            self.stop_event.set()
            for p in self.bg_processes:
                p.join()

# ---------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------
if __name__ == "__main__":
    world = World()

    # choose either local or multiprocess pipe
    # emitter, receiver = world.local_pipe()
    #emitter, receiver = world.mp_pipe()
    emitter, receiver = world.local_pipe()

    # Background sensor runs in a subprocess
    bg_loops = [(sensor_loop, (emitter,))]

    # Controller runs in main process
    main_loops = [(controller_loop, (receiver,))]

    world.start(main_loops, bg_loops)


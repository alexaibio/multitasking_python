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


@dataclass
class Message:
    data: any
    ts: float


class TransportMode(IntEnum):
    UNDECIDED = 0
    QUEUE = 1
    SHARED_MEMORY = 2  # Not really used here, just to mirror your structure


class LocalQueueEmitter:
    def __init__(self, queue: deque):
        self.queue = queue

    def emit(self, data):
        msg = Message(data=data, ts=time.time())
        self.queue.append(msg)
        print(f"[Emitter] Emitted {data} at {msg.ts:.2f}")


class LocalQueueReceiver:
    def __init__(self, queue: deque):
        self.queue = queue
        self.last_msg = None

    def read(self):
        if self.queue:
            self.last_msg = self.queue.popleft()
        return self.last_msg


class BroadcastEmitter:
    """Broadcasts one message to multiple emitters."""
    def __init__(self, emitters):
        self.emitters = emitters

    def emit(self, data):
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

    # Create a communication channel (local queue)
    emitter, receiver = world.local_pipe()

    # Background sensor runs in a subprocess
    bg_loops = [(sensor_loop, (emitter,))]

    # Controller runs in main process
    main_loops = [(controller_loop, (receiver,))]

    world.start(main_loops, bg_loops)


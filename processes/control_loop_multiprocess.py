import multiprocessing as mp
import random
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class Message:
    data: float
    ts: int     # timestamp in nanoseconds


# --- Signal primitives ---
class SignalEmitter:
    """Emits messages into a multiprocessing-safe queue."""
    def __init__(self, queue: mp.Queue):
        self.queue = queue

    def emit(self, data: float):
        ts = int(time.time() * 1e9)
        msg = Message(data, ts)
        self.queue.put(msg)
        # non-blocking emit
        print(f"[Emitter] Sent value {data:.2f} (ts={ts})")


class SignalReceiver:
    """Reads messages from a multiprocessing-safe queue."""
    def __init__(self, queue: mp.Queue):
        self.queue = queue

    def read(self) -> Optional[Message]:
        try:
            msg = self.queue.get_nowait()
            return msg
        except Exception:
            return None


# --- The 'World' orchestrator ---
class World:
    """Very simple orchestrator that connects systems and runs them."""
    def __init__(self):
        self.processes: list[mp.Process] = []
        self.stop_event = mp.Event()

    def connect(self) -> tuple[SignalEmitter, SignalReceiver]:
        """Create a queue and return paired emitter and receiver."""
        q = mp.Queue(maxsize=10)
        return SignalEmitter(q), SignalReceiver(q)

    def run_background(self, target, *args):
        p = mp.Process(target=target, args=args)
        p.start()
        self.processes.append(p)

    def stop_all(self):
        self.stop_event.set()

    def join_all(self):
        for p in self.processes:
            p.join()


# --- Sensor (producer) ---
def sensor_loop(emitter: SignalEmitter, stop_event: mp.Event):
    print("[Sensor] Started")
    while not stop_event.is_set():
        value = 20 + random.random() * 5  # Simulated temperature
        emitter.emit(value)
        time.sleep(0.5)
    print("[Sensor] Stopped")


# --- Controller (consumer) ---
def controller_loop(receiver: SignalReceiver, stop_event: mp.Event):
    print("[Controller] Started")
    while not stop_event.is_set():
        msg = receiver.read()
        if msg:
            reaction = "Cooling ON" if msg.data > 22.5 else "Cooling OFF"
            print(f"[Controller] Temp={msg.data:.2f} °C → {reaction}")
        time.sleep(0.2)
    print("[Controller] Stopped")


# --- Main orchestration ---
if __name__ == "__main__":
    world = World()
    emitter, receiver = world.connect()

    # Run controller in background
    world.run_background(controller_loop, receiver, world.stop_event)

    # Run sensor in main process
    try:
        sensor_loop(emitter, world.stop_event)
    except KeyboardInterrupt:
        print("\n[Main] Interrupted, stopping...")
        world.stop_all()

    # Clean up
    world.join_all()
    print("[Main] Finished")

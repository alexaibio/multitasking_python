import multiprocessing as mp
import random
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class Message:
    data: float
    ts: int  # timestamp in nanoseconds


# --- Signal primitives ---
class SignalEmitter:
    """Emits messages into a multiprocessing-safe queue."""
    def __init__(self, queue: mp.Queue):
        self.queue = queue

    def emit(self, data: float):
        ts = int(time.time() * 1e9)
        msg = Message(data, ts)
        self.queue.put(msg)
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
    """Orchestrator that connects systems, manages processes, and provides lifecycle control."""
    def __init__(self):
        self.processes: list[mp.Process] = []
        self.stop_event = mp.Event()

    def __enter__(self):
        print("[World] Entered context")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("[World] Exiting context...")
        self.stop_all()
        self.join_all()
        print("[World] All processes cleaned up")

    def connect(self) -> tuple[SignalEmitter, SignalReceiver]:
        """Create a queue and return paired emitter and receiver."""
        q = mp.Queue(maxsize=10)
        return SignalEmitter(q), SignalReceiver(q)

    def run_background(self, target, *args):
        """Launch a target function in a background process."""
        p = mp.Process(target=target, args=args)
        p.start()
        print(f"[World] Started background process {p.name} (pid={p.pid})")
        self.processes.append(p)

    def stop_all(self):
        """Signal all running systems to stop."""
        print("[World] Sending stop signal to all systems...")
        self.stop_event.set()

    def join_all(self):
        """Wait for all background processes to finish."""
        for p in self.processes:
            p.join()
            print(f"[World] Process {p.name} finished")


# --- Sensor (producer) ---
def sensor_loop(emitter: SignalEmitter, stop_event: mp.Event):
    print("[Sensor] Started")
    try:
        while not stop_event.is_set():
            value = 20 + random.random() * 5  # Simulated temperature
            emitter.emit(value)
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
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
    with World() as world:
        emitter, receiver = world.connect()

        # Run controller in background
        world.run_background(controller_loop, receiver, world.stop_event)

        # Run sensor in main process
        try:
            sensor_loop(emitter, world.stop_event)
        except KeyboardInterrupt:
            print("\n[Main] Interrupted by user.")
            world.stop_all()

    print("[Main] Finished")

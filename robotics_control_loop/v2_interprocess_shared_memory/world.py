"""
- Ran every generator every iteration
- Blocked on time.sleep(command.seconds) inside each loop
- Did not coordinate sleep across tasks
- Could not determine the next earliest task to run

"""

from collections import deque
import heapq
import multiprocessing as mp
import time
from typing import Any, Generic, List, Tuple, TypeVar

from interprocess_shared_memory import (
    Clock, Sleep, TransportMode, ParallelismType,
    Sensor, CloudinessSensor, TemperatureSensor, CameraSensor,
    SignalEmitter, SignalReceiver, LocalQueueReceiver, LocalQueueEmitter, MultiprocessEmitter, MultiprocessReceiver,
    _bg_wrapper_loop, sensor_gen_fn, controller_gen_fn
)


# ---------------------------------------------------------------------
class World:
    """ Scheduler: orchestrating control loops """
    def __init__(self, clock):
        self._stop_event = mp.Event()
        self.background_processes = []
        self.clock = clock
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
            up_value = self._manager.Value('b', False)  # a flag that SM block has a new FRESH value
            sm_queue = self._manager.Queue()            # a separate queue to send Shared Memory metadata

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

        self._cleanup_resources.append((emitter, receiver))     # we need to clean shared memory at the end
        return emitter, receiver

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    def run_blocking_sleep(self, fg_loops, bg_loops):
        print(f"[world] is starting")

        ### Start background interprocess loops
        for background_fn, args in bg_loops:
            pr = mp.Process(target=_bg_wrapper_loop, args=(background_fn, self._stop_event, *args))
            pr.start()
            self.background_processes.append(pr)
            print(f"[World] Started background process for {args[1].sensor_id}, {args[1].transport.name}")

        #### Run main loop (cooperative scheduling - coroutines) inside the main process
        try:         # if KeyboardInterrupt stop all processes
            generators = [
                cooperative_fn(self._stop_event, *args)
                for cooperative_fn, args in fg_loops
            ]
            # Initialize scheduling of the next reading
            all_next_times = {next_time: 0.0 for next_time in generators}

            ################
            while not self._stop_event.is_set():
                now = self.clock.now_ns()

                for gen in generators:
                    if now >= all_next_times[gen]:
                        command = next(gen)
                        if isinstance(command, Sleep):
                            time.sleep(command.seconds)
                        else:
                            raise ValueError(f" Unexpected command {command}")
            ################
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

    def run_cooperative(self, fg_loops, bg_loops):
        print(f"[world] is starting")

        ### Start background interprocess loops
        for background_fn, args in bg_loops:
            pr = mp.Process(target=_bg_wrapper_loop, args=(background_fn, self._stop_event, *args))
            pr.start()
            self.background_processes.append(pr)
            print(f"[World] Started background process for {args[1].sensor_id}, {args[1].transport.name}")

        #### Run foreground cooperative scheduling loop inside the main process
        try:         # if KeyboardInterrupt -> stop and clean all processes
            generators = [
                cooperative_fn(self._stop_event, *args)
                for cooperative_fn, args in fg_loops
            ]

            priority_queue = []
            counter = 0
            clock = self.clock
            now = clock.now_ns()

            # First scheduling: start all generators immediately
            for gen in generators:
                priority_queue.append((now, counter, gen))
                counter += 1
            heapq.heapify(priority_queue)

            ######################
            while priority_queue and not self._stop_event.is_set():
                # get the closest scheduled gen
                next_time, _, gen = heapq.heappop(priority_queue)

                # wait until its scheduled execution time
                # note: we are not spinning the loop if no reading were scheduled - optimal!
                current = clock.now_ns()
                wait_ns = max(0, next_time - current)
                if wait_ns > 0:
                    time.sleep(wait_ns / 1e9)

                command = next(gen)     # advance current generator (read sensor)
                if not isinstance(command, Sleep):
                    raise ValueError(f"Unexpected command {command}")

                # reschedule this generator based on yielded sleep
                run_at = clock.now_ns() + int(command.seconds * 1e9)
                heapq.heappush(priority_queue,(run_at, counter, gen))
                counter += 1
            #######################


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
    PARALLELISM_TYPE = ParallelismType.MULTIPROCESS     # LOCAL / MULTIPROCESS

    sensors: list[Sensor] = [
        TemperatureSensor(transport=TransportMode.QUEUE, interval=5.0),
        CloudinessSensor(transport=TransportMode.QUEUE, interval=10.0),
        #CameraSensor(transport=TransportMode.QUEUE, interval=1.0)
        CameraSensor(transport=TransportMode.SHARED_MEMORY, interval=1.0)
    ]

    # Sanity check: Ensure SM only used in MULTIPROCESS mode
    if PARALLELISM_TYPE == ParallelismType.LOCAL and (
            wrong_s := [s for s in sensors if s.transport==TransportMode.SHARED_MEMORY]):
        raise ValueError(f"SHARED_MEMORY transport not allowed in LOCAL mode for {[s.sensor_id for s in wrong_s]}")



    #### create World environment
    clk = Clock()       # we might use a custom clock
    world = World(clk)

    # create pipes between sensor and controller (emitter -> receiver)
    pipes: list[Tuple[Sensor, SignalEmitter, SignalReceiver]] = []
    for sensor in sensors:
        if PARALLELISM_TYPE == ParallelismType.LOCAL:
            emitter, receiver = world.local_pipe()
        else:
            emitter, receiver = world.mp_pipe(sensor.transport)
        pipes.append((sensor, emitter, receiver))

    # list of necessary emitting loops
    emitting_loops = [
        (sensor_gen_fn, (emitter, sensor))
        for sensor, emitter, _ in pipes
    ]

    # Single controller loop with all receivers in it (because it is always a cooperative loop)
    receivers = [(sensor, receiver) for sensor, _, receiver in pipes]
    controller_loop = [
        (controller_gen_fn, (receivers,))
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
    #world.run_blocking_sleep(cooperative_loops, bg_loops)
    world.run_cooperative(cooperative_loops, bg_loops)
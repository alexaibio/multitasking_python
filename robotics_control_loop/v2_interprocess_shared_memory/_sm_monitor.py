"""
You can run this monitor to observe is camera data is really transmitted over Shared Memory
Note
    shm_name = "psm_99f8f26d" from printout of main script like
    [Emitter] Created SM buffer and sent its metadata: psm_aea76689, size=192000
"""


from multiprocessing import shared_memory
import numpy as np
import time


shm_name = "psm_99f8f26d"

try:
    sm = shared_memory.SharedMemory(name=shm_name)
    try:
        while True:
            arr = np.ndarray((320, 200, 3), dtype=np.uint8, buffer=sm.buf)
            print("Shared ARRAY:", arr[0,0])
            print(f" raw SM: {sm}")
            time.sleep(3)
    except KeyboardInterrupt:
        sm.close()  # detach after use

except FileNotFoundError:
    print(f"[Monitor] Shared memory {shm_name} not found.")
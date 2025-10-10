"""
all threads have access to the same data (as they belong to the same process),
executing multiple threads at the same time can cause data issues.

This problem is commonly known as a race condition.
"""
import threading
import time
from threading import Thread


shared_data = 0

def multiply_data(nums):
    global shared_data
    for i in range(nums):
        shared_data += 1
        time.sleep(3)

#nums = 10
#threads = [Thread(target=multiply_data, args=(nums,)) for t in range(5)]

#[t.start() for t in threads]

# по идее каждый из 5 процессов долен прибавить по 10 - в сумме 50... но нет
#assert shared_data == len(threads) * nums
#print(f'shared data 1: {shared_data}')

# Solution : use Lock
# The idea is that each thread should acquire the lock if the lock is free.
# If the lock is busy (i.e. it was acquired by a different thread), the other threads have to wait for the lock to be released.

def multiply_with_lock(nums, lock):
    global shared_data
    for i in range(nums):
        lock.acquire()
        shared_data += 1
        lock.release()
        time.sleep(1)
        # althernatively
        # with self.lock:
        #   shared_data += 1


lock = threading.Lock()
shared_data = 0
nums = 10

# запускаем 5 потоков и каждый жолжен слжоить 10 единиц
threads = [Thread(target=multiply_with_lock, args=(nums, lock)) for t in range(5)]
[t.start() for t in threads]

# we need it because aothrwise we will check assert before all threads are finished
[t.join() for t in threads]

print(f'shared data 2: {shared_data}')
assert shared_data == len(threads) * nums

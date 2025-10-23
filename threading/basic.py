import time
import random
import sys
from threading import Thread


def my_function():
    print(" I started...")
    time.sleep(3)
    print('.. awake')

def rand_generator():
    n = random.randint(0,50)
    print(f' My random numer {n}')
    time.sleep(5)
    print(f'...end of {n}')


########## MAIN script

# create 5 threads
threads = [Thread(target=rand_generator) for i in range(5)]
print(threads[0].is_alive())

# start all threads
[t.start() for t in threads]
print(f'\n {threads[0].is_alive()}' )


# если для дальгейшей работы программы нужны результаты всех потоков - join
print('Join to all threads, so we will wait while they finish...')
[t.join() for t in threads]
print(' Now we can continue execution, all threads are finished')
import time
import random
import sys
from threading import Thread

def my_function():
    print(" I started...")
    time.sleep(3)
    print('.. awake')

#t1 = Thread(target=my_function)
#t1.start()
#print(t1.isAlive())

def rand_generator():
    n = random.randint(0,50)
    print(f' My random numer {n}')
    time.sleep(5)
    print(f'...end of {n}')

threads = [Thread(target=rand_generator) for i in range(7)]

print(threads[0].is_alive())
[t.start() for t in threads]
print(f'\n {threads[0].is_alive()}' )
print(' все потоки запущены')

# если для дальгейшей работы программы нужны результаты всех потоков - join
print('------ now join -------> ждем пока все потоки закончатся - чтобы одновлеменно полуить результаты')
[t.join() for t in threads]
print(' вот тут все потоки закончились')
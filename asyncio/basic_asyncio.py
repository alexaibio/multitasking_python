import asyncio
import time


# A fast co-routine
async def add_fast(x, y):
    print("starting fast add")
    await asyncio.sleep(3)          # cooperative non-blocking pause
    print("result ready for FAST add")
    return x + y

# A slow co-routine
async def add_slow(x, y):
    print("starting slow add")
    await asyncio.sleep(5)          # cooperative non-blocking pause
    print("result ready for SLOW add")
    return x + y


# Legacy blocking function (BAD)
def old_blocking_bad_function():
    print("starting OLD blocking function (will block loop if called directly)")
    time.sleep(4)               # blocking pause
    print("OLD blocking function finished")
    return "blocking result"

# run blocking code in thread
async def blocking_good():
    print("\n--- GOOD: using to_thread ---")
    result = await asyncio.to_thread(old_blocking_bad_function)
    print("blocking returned:", result)



# Create a function to schedule co-routines
async def get_results(mode: str):
    if mode=='background/concurrent work':
        # background/concurrent work
        task1 = asyncio.create_task(add_slow(3, 4))
        task2 = asyncio.create_task(add_fast(5, 5))
        task3 = asyncio.create_task(blocking_good())

        print(await task1, await task2, await task3)     # Prints only when both are finished
    else:
        # print only when both are finished
        results = await asyncio.gather(
            add_slow(3, 4),
            add_fast(5, 5),
        )
        print(*results)  # also prints when both are finished



asyncio.run(get_results(mode='background/concurrent work'))
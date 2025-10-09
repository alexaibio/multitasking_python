import asyncio

# A fast co-routine
async def add_fast(x, y):
    print("starting fast add")
    await asyncio.sleep(3) # Mimic some network delay
    print("result ready for FAST add")
    return x + y

# A slow co-routine
async def add_slow(x, y):
    print("starting slow add")
    await asyncio.sleep(5) # # Mimic some network delay
    print("result ready for SLOW add")
    return x + y

# Create a function to schedule co-routines
async def get_results():
    task1 = asyncio.create_task(add_slow(3, 4))
    task2 = asyncio.create_task(add_fast(5, 5))

    print(await task1, await task2) # Prints 7 10

asyncio.run(get_results())
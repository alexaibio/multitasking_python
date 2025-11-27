# Toy examples of how robotics loops are typically organized.

## Table of Content
- cooperative scheduling: a way to asquaring data from sensors in single thread
  - `cooperative_scheduling_sensor_time.py` - a toy example of cooperative scheduling with time controlled by sensor loop. All in one thread.
  - `cooperative_scheduling_sensor_asyncio.py` - same but with asyncio framework
  - `cooperative_scheduling_world_time.py` - same but with time controlled by World, not sensor loop
- multiprocess interaction of different sensors in different process with shared memory or queue
  - `interprocess_shared_memory.py`
- Full scale robotic control system
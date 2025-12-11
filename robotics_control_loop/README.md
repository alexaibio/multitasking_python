# Robotics from first principals: toy examples

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## Intro
In robotics, many things happen simultaneously — sensors gather data, motors respond, and controllers make decisions — and we need to process them appropriately. This is one of the main challenges in robotics programming.

The naïve idea is simple: just read all sensors one at a time in an event loop, as frequently as possible.

That approach works, but it has several drawbacks. For example, each sensor may need to be read at a different frequency. It’s enough to read a temperature sensor once a minute, but a video camera or accelerometer must be read much more often. Reading all sensors at the same frequency causes unnecessary CPU overhead and power consumption — which are critical issues for embedded systems.


## Table of content
- [Intro](#intro)
- [Cooperative Scheduling](#cooperative-scheduling)
  - [How Cooperative Scheduling Is Organized Programmatically](#how-cooperative-scheduling-is-organized-programmatically)
- [Combining Cooperative Scheduling with Multiprocessing](#combining-cooperative-scheduling-with-multiprocessing)
- [Examples](#examples)


## Cooperative scheduling
One way to solve this problem is cooperative scheduling. 

Cooperative scheduling strategy — a way to organize concurrent tasks so that each sensor loop voluntarily gives up control (cooperates) rather than being forcibly interrupted (preempted).

A cooperative scheduler stands in contrast to preemptive scheduling.

In preemptive scheduling, you run each sensor-reading loop in a separate thread or process, and the operating system (OS) scheduler decides when a thread or process has run long enough and should pause to let another run — even if the first thread hasn’t finished its current operation.


### How Cooperative Scheduling Is Organized Programmatically

Each sensor is read in a loop. After its first reading, it sends (emits) a message to the controller loop (the receiver) through a pipe, which is essentially a queue.

During each iteration, the sensor (emitter) sends not only the reading itself but also a command to the controller — usually a Sleep command, but it could also be something else (e.g., logging or control instructions).

Note that Sleep command is not an actual OS sleep — it’s an instruction interpreted by the controller to decide when to next schedule the sensor.

Overall, in each loop iteration, every sensor sends both:

Its reading (e.g., temperature = 22.4°C), and

A command, like Sleep(2), meaning “wait 2 seconds before asking me again.”

Since the sensor loop is organized as a generator, it stops at the yield Sleep(1) statement, gives control back to the controller loop, and waits until the controller resumes it for the next iteration.
This means the sensor is not running on every tick (loop iteration) — avoiding unnecessary energy and CPU consumption — but only when necessary for that specific sensor.

Here is a very simple example of how cooperative scheduling is organized
**[cooperative_scheduling_sensor_time.py](https://github.com/alexaibio/multitasking_python/blob/main/robotics_control_loop/v1_cooperative_scheduling/cooperative_scheduling_sensor_time.py)**



## Combining Cooperative Scheduling with Multiprocessing

Now, let's try to scale this approach up a little and demonstrate how a larger system might be designed by utilizing both a cooperative loop (for controllers and non-demanding sensors) and a multiprocessing setup, where demanding sensors such as a camera run in a separate process and send data via a shared variable or a shared memory block.

In principle, the camera could also use a multiprocessing queue. However, since it sends a large amount of data every second, the overhead of using a queue would be enormous. Therefore, using shared memory is a more efficient way to handle such a high data throughput between processes.

The full example is here - https://github.com/alexaibio/multitasking_python/tree/main/robotics_control_loop/v2_interprocess_shared_memory

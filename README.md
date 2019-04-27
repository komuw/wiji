## wiji          


[![Codacy Badge](https://api.codacy.com/project/badge/Grade/f0b4b7a07da24e90bdb7743d0e6b9240)](https://www.codacy.com/app/komuw/wiji)
[![CircleCI](https://circleci.com/gh/komuw/wiji.svg?style=svg)](https://circleci.com/gh/komuw/wiji)
[![codecov](https://codecov.io/gh/komuw/wiji/branch/master/graph/badge.svg)](https://codecov.io/gh/komuw/wiji)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/komuw/wiji)



`Wiji` is an asyncio distributed task processor/queue.       
It's name is derived from the late Kenyan hip hop artiste, Gwiji.      

It is a bit like [Celery](https://github.com/celery/celery)        
 
`wiji` has no third-party dependencies and it requires python version 3.7+
  
`wiji` is work in progress and very early. It's API may change in backward incompatible ways.              
[https://pypi.python.org/pypi/wiji](https://pypi.python.org/pypi/wiji)                 

**Contents:**          
[Installation](#installation)         
[Usage](#usage)                  
  + [As a library](#1-as-a-library)            
  + [As cli app](#2-as-a-cli-app)            

[Features](#features)               
  + [async everywhere](#1-async-everywhere)            
  + [monitoring-and-observability](#2-monitoring-and-observability)            
    + [logging](#21-logging)            
    + [hooks](#22-hooks)
  + [Rate limiting](#3-rate-limiting)                     
  + [Queuing](#5-queuing)            


## Installation

```shell
pip install wiji
```           


## Usage

#### 1. As a library
```python
import asyncio
import wiji

class AdderTask(wiji.task.Task):
    the_broker = wiji.broker.InMemoryBroker()
    queue_name = "AdderTaskQueue1"

    async def run(self, a, b):
        result = a + b
        print("\nresult: {0}\n".format(result))
        return result

# queue some tasks
myAdderTask = AdderTask( )
myAdderTask.synchronous_delay(a=4, b=37)
myAdderTask.synchronous_delay(a=67, b=847)

# run the workers
worker = wiji.Worker(the_task=myAdderTask)
asyncio.run(worker.consume_tasks())
```

#### 2. As a cli app
`wiji` also ships with a commandline app called `wiji-cli`.             
                
create a `wiji` app file(which is just any python file that has a class instance of `wiji.app.App`), eg;             
`examples/my_app.py`                 
```python
import wiji

class AdderTask(wiji.task.Task):
    the_broker = wiji.broker.InMemoryBroker()
    queue_name = "AdderTaskQueue1"

    async def run(self, a, b):
        res = a + b
        print()
        print("res:: ", res)
        print()
        return res

MyAppInstance = wiji.app.App(task_classes=[AdderTask])
```          
**NB:** the directory where your place that file(in this case; `examples/`) ought to be in your `PYTHONPATH`               
then run `wiji-cli` pointing it to the dotted path of the `wiji.app.App` instance:     

```bash
wiji-cli --app examples.my_app.MyAppInstance
```

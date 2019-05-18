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
  + [Writing tests](#writing-tests)             

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

#### Writing tests
Lets say you have `wiji` tasks in your project and you want to write integration or unit tests for them and their use.     
```python

# my_tasks.py

import wiji
import MyRedisBroker # a custom broker using redis

DATABASE = {}

class AdderTask(wiji.task.Task):
    the_broker = MyRedisBroker()
    queue_name = "AdderTask"

    async def run(self, a, b):
        """
        adds two numbers and stores the resut in a database
        """
        result = a + b
        DATABASE["result"] = result
        return result

class ExampleView:
    def post(self, request):
        a = request["a"]
        b = request["b"]
        AdderTask().synchronous_delay(a=a, b=b)
```
In the example above we have a view with one `post` method. When that method is called it queues a task that adds two numbers and then stores the result of that addition in a database.    
That task uses a broker(`MyRedisBroker`) that is backed by redis.    
One way to write your tests would be;    
```python

# test_tasks.py

from my_tasks import ExampleView
from unittest import TestCase

class TestExampleView(TestCase):
    def test_view(self):
        view = ExampleView()
        view.post(request={"a": 45, "b": 46})
        # do your asserts here
```
The problem with the above approach is that this will require you to have an instance of redis running for that test to run succesfully.   
This may not be what you want. Ideally you do not want your tests be dependent on external services.    
`wiji` ships with an in-memory broker that you can use in your tests.   
So the test above can be re-written in this manner;
```python

# test_tasks.py

import asyncio
from my_tasks import ExampleView, AdderTask, DATABASE
from unittest import TestCase, mock

class TestExampleView(TestCase):

    @staticmethod
    def _run(coro):
        """
        helper function that runs any coroutine in an event loop.
        see:: https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code
        """
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

    def test_view(self):
        with mock.patch.object(
            # ie, substitute the redis broker with an in-memory one during test runs
            AdderTask, "the_broker", wiji.broker.InMemoryBroker()
        ) as mock_broker:
            view = ExampleView()
            view.post(request={"a": 45, "b": 46})
            # do your asserts here
    
    def test_whole_flow(self):
        with mock.patch.object(
            AdderTask, "the_broker", wiji.broker.InMemoryBroker()
        ) as mock_broker:
            # 1. assert that the database is initially empty
            self.assertDictEqual(DATABASE, {})

            # 2. when `post` is called it will queue a task
            view = ExampleView()
            view.post(request={"a": 45, "b": 46})
            
            # 3. we need to run workers
            worker = wiji.Worker(the_task=AdderTask())
            self._run(worker.consume_tasks(TESTING=True))
            
            # 4. assert that database has been updated succesfully.
            self.assertDictEqual(DATABASE, {"result": 91})
```
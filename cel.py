import celery


"""
run as:
    python cel.py &; celery worker -A cel:celobj
      or
    celery worker -A cel:celobj --concurrency=1 --soft-time-limit=25 --loglevel=INFO --pool=komupool
"""
celobj = celery.Celery()


@celobj.task(name="sync_req")
def sync_req(url):
    import requests

    res = requests.get(url=url, timeout=90)
    print("res: ", res)


@celobj.task(name="Async_req", this_is_async_task=True)
async def Async_req(url):
    import asyncio
    import aiohttp

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            print("resp statsus: ", resp.status)
            res_text = await resp.text()
            print(res_text[:50])


# from celery.exceptions import Retry

import inspect

# @celobj.task(name="adder", bind=True, throw=True)

FUNHEAD_TEMPLATE = """def {fun_name}({fun_args}):\n    return {fun_value}"""


def adder(a, b):
    func_name = adder.__name__
    fun_args = ", ".join(inspect.getfullargspec(adder).args)
    fun_value = 459_029
    definition = FUNHEAD_TEMPLATE.format(fun_name=func_name, fun_args=fun_args, fun_value=fun_value)

    namespace = {"__name__": adder.__module__}
    exec(definition, namespace)

    result = namespace[func_name]
    result._source = definition

    assert result(4, 89) == fun_value
    assert result(4, 2333) == fun_value
    assert result("PPL", "rwr") == fun_value
    assert result("PPL", "rwr", 90, "LL") == fun_value

    # import pdb

    # pdb.set_trace()
    # inspect.getfullargspec(adder).args[0] == 'a'
    res = a + b
    print()
    print("adder: ", res)

    return None


from celery.utils.functional import first, head_from_fun

head_from_fun(adder, bound=False)


@celobj.task(name="divider")
def divider(a):
    res = a / 3
    print()
    print("divider: ", res)
    return res


if __name__ == "__main__":
    # sync_req.delay(url="https://httpbin.org/delay/5")
    # print("!!! SYNC messages enqueued !!!")

    # # for _ in range(0, 10):
    # Async_req.delay(url="https://httpbin.org/delay/7")
    # print("!!! Async messages enqueued !!!")
    x = adder(45, 47)
    # chain = adder.s(3, 7) | divider.s()
    # chain()
    print("!!! chain enqueued !!!")
    """
    The above enques only ONE task, the adder task with a body payload like.
        [
        [
            3,
            7
        ],
        {},
        {
            "callbacks": null,
            "errbacks": null,
            "chain": [
                {
                    "task": "divider",
                    "args": [],
                    "kwargs": {},
                    "options": {
                    "queue": "divider",
                    "task_id": "05bd2f0f-de20-498b-bc96-75c8a42fcb99",
                    "reply_to": "93db68e0-9696-3f15-ac26-29e44f857c33"
                    },
                    "subtask_type": null,
                    "chord_size": null,
                    "immutable": false
                }
            ],
            "chord": null
        }
        ]
    """


import inspect


FUNHEAD_TEMPLATE = """
def {fun_name}({fun_args}):
    return {fun_value}
"""


def _argsfromspec(spec, replace_defaults=True):
    if spec.defaults:
        split = len(spec.defaults)
        defaults = list(range(len(spec.defaults))) if replace_defaults else spec.defaults
        positional = spec.args[:-split]
        optional = list(zip(spec.args[-split:], defaults))
    else:
        positional, optional = spec.args, []

    varargs = spec.varargs
    varkw = spec.varkw
    if spec.kwonlydefaults:
        split = len(spec.kwonlydefaults)
        kwonlyargs = spec.kwonlyargs[:-split]
        if replace_defaults:
            kwonlyargs_optional = [(kw, i) for i, kw in enumerate(spec.kwonlyargs[-split:])]
        else:
            kwonlyargs_optional = list(spec.kwonlydefaults.items())
    else:
        kwonlyargs, kwonlyargs_optional = spec.kwonlyargs, []

    return ", ".join(
        filter(
            None,
            [
                ", ".join(positional),
                ", ".join("{0}={1}".format(k, v) for k, v in optional),
                "*{0}".format(varargs) if varargs else None,
                "*" if (kwonlyargs or kwonlyargs_optional) and not varargs else None,
                ", ".join(kwonlyargs) if kwonlyargs else None,
                ", ".join('{0}="{1}"'.format(k, v) for k, v in kwonlyargs_optional),
                "**{0}".format(varkw) if varkw else None,
            ],
        )
    )


def head_from_fun(fun, debug=False):
    """Generate signature function from actual function."""
    # we could use inspect.Signature here, but that implementation
    # is very slow since it implements the argument checking
    # in pure-Python.  Instead we use exec to create a new function
    # with an empty body, meaning it has the same performance as
    # as just calling a function.
    is_function = inspect.isfunction(fun)
    is_callable = hasattr(fun, "__call__")
    is_cython = fun.__class__.__name__ == "cython_function_or_method"
    is_method = inspect.ismethod(fun)

    if not is_function and is_callable and not is_method and not is_cython:
        name, fun = fun.__class__.__name__, fun.__call__
    else:
        name = fun.__name__
    definition = FUNHEAD_TEMPLATE.format(
        fun_name=name, fun_args=_argsfromspec(getfullargspec(fun)), fun_value=1
    )
    if debug:  # pragma: no cover
        print(definition)
    namespace = {"__name__": fun.__module__}
    # pylint: disable=exec-used
    # Tasks are rarely, if ever, created at runtime - exec here is fine.
    exec(definition, namespace)
    result = namespace[name]
    result._source = definition

    return result

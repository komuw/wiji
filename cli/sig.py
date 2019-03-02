import os
import sys
import json
import string
import signal
import random
import typing
import asyncio
import inspect
import logging
import argparse
import functools


# TODO: this functions should live in their own file
async def _signal_handling(logger, workers):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()

    try:
        for signal_number in [signal.SIGHUP, signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]:
            loop.add_signal_handler(
                signal_number,
                functools.partial(
                    asyncio.ensure_future,
                    _handle_termination_signal(
                        logger=logger, signal_number=signal_number, workers=workers
                    ),
                ),
            )
    except ValueError as e:
        logger.log(
            logging.DEBUG,
            {
                "event": "wiji.cli.signals",
                "stage": "end",
                "state": "this OS does not support the said signal",
            },
        )


async def _handle_termination_signal(logger, signal_number, workers):
    signal_table = {1: "SIGHUP", 2: "SIGINT", 3: "SIGQUIT", 9: "SIGKILL", 15: "SIGTERM"}
    # TODO: add debug logging

    logger.log(
        logging.DEBUG,
        {
            "event": "wiji.cli.signals",
            "stage": "start",
            "state": "received termination signal",
            "signal_number": signal_number,
            "signal_name": signal_table.get("signal_number", "unknown signal"),
        },
    )

    shutdown_tasks = []
    for worker in workers:
        shutdown_tasks.append(worker.shutdown())
    # the shutdown process for all tasks needs to happen concurrently
    tasks = asyncio.gather(*shutdown_tasks)
    asyncio.ensure_future(tasks)

    def wait_worker_shutdown(workers) -> bool:
        no_workers = len(workers)
        success_shut_down = 0
        for worker in workers:
            if worker.SUCCESFULLY_SHUT_DOWN:
                success_shut_down += 1

        if success_shut_down == no_workers:
            loop_continue = False
        else:
            loop_continue = True
        return loop_continue

    while wait_worker_shutdown(workers=workers):
        logger.log(
            logging.DEBUG,
            {
                "event": "wiji.cli.signals",
                "stage": "start",
                "state": "waiting for all workers to drain properly",
            },
        )
        await asyncio.sleep(5)

    logger.log(
        logging.DEBUG,
        {
            "event": "wiji.cli.signals",
            "stage": "end",
            "state": "all workers have succesfully shutdown",
        },
    )
    return

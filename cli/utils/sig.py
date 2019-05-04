import signal
import asyncio
import logging
import functools

import wiji


async def _signal_handling(logger: wiji.logger.BaseLogger, workers: list) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()

    try:
        for _signal in [signal.SIGHUP, signal.SIGQUIT, signal.SIGTERM]:
            loop.add_signal_handler(
                _signal,
                functools.partial(
                    asyncio.ensure_future,
                    _handle_termination_signal(logger=logger, _signal=_signal, workers=workers),
                ),
            )
    except ValueError as e:
        logger.log(
            logging.DEBUG,
            {
                "event": "wiji.cli.signals",
                "stage": "end",
                "state": "this OS does not support the said signal",
                "error": str(e),
            },
        )


async def _handle_termination_signal(
    logger: wiji.logger.BaseLogger, _signal: int, workers: list
) -> None:
    logger.log(
        logging.INFO,
        {
            "event": "wiji.cli.signals",
            "stage": "start",
            "state": "received termination signal",
            "signal": _signal,
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
            logging.INFO,
            {
                "event": "wiji.cli.signals",
                "stage": "start",
                "state": "waiting for all workers to drain properly",
            },
        )
        await asyncio.sleep(5)

    logger.log(
        logging.INFO,
        {
            "event": "wiji.cli.signals",
            "stage": "end",
            "state": "all workers have succesfully shutdown",
        },
    )
    return

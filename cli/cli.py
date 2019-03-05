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

import wiji
from cli import utils


os.environ["PYTHONASYNCIODEBUG"] = "1"


def make_parser():
    """
    this is abstracted into its own method so that it is easier to test it.
    """
    parser = argparse.ArgumentParser(
        prog="wiji",
        description="""wiji is an async distributed task queue.
                example usage:
                wiji-cli \
                --config /path/to/my_config.json
                """,
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=wiji.__version__.about["__version__"]),
        help="The currently installed wiji version.",
    )
    parser.add_argument(
        "--config",
        required=True,
        type=argparse.FileType(mode="r"),
        help="The config file to use. \
        eg: --config /path/to/my_config.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        required=False,
        default=False,
        help="""Whether we want to do a dry-run of the wiji cli.
        This is typically only used by developers who are developing wiji.
        eg: --dry-run""",
    )
    return parser


def main():
    """
    run as:
        wiji-cli --config /path/to/my_config.json
    """
    worker_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))
    logger = wiji.logger.SimpleBaseLogger("wiji.cli")
    logger.log(logging.INFO, {"event": "wiji.cli.main", "stage": "start", "worker_id": worker_id})
    try:
        parser = make_parser()
        args = parser.parse_args()

        dry_run = args.dry_run
        config = args.config
        config_contents = config.read()
        # todo: validate that config_contents hold all the required params
        kwargs = json.loads(config_contents)

        config_tasks = kwargs["tasks"]  # this is a mandatory param
        list_of_tasks = []
        for config_tsk in config_tasks:
            task = utils.load.load_class(config_tsk)
            if inspect.isclass(task):
                # DO NOT instantiate class instance, fail with appropriate error instead.
                err_msg = "task should be a class instance."
                logger.log(
                    logging.ERROR, {"event": "wiji.cli.main", "stage": "end", "error": err_msg}
                )
                sys.exit(77)
            list_of_tasks.append(task)

        async def async_main():
            workers = [wiji.Worker(the_task=wiji.task.WatchDogTask, use_watchdog=True)]
            producers = [utils._producer.produce_tasks_continously(task=wiji.task.WatchDogTask)]

            for task in list_of_tasks:
                _worker = wiji.Worker(the_task=task)
                workers.append(_worker)

            consumers = []
            for i in workers:
                consumers.append(i.consume_tasks())

            gather_tasks = asyncio.gather(
                *consumers, *producers, utils.sig._signal_handling(logger=logger, workers=workers)
            )
            await gather_tasks

        asyncio.run(async_main(), debug=True)
    except Exception as e:
        logger.log(logging.ERROR, {"event": "wiji.cli.main", "stage": "end", "error": str(e)})
        sys.exit(77)
    finally:
        logger.log(logging.INFO, {"event": "wiji.cli.main", "stage": "end"})


if __name__ == "__main__":
    main()

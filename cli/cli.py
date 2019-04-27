import os
import sys
import typing
import string
import random
import asyncio
import logging
import argparse

import wiji
from cli import utils


os.environ["PYTHONASYNCIODEBUG"] = "1"


def make_parser() -> argparse.ArgumentParser:
    """
    this is abstracted into its own method so that it is easier to test it.
    """
    parser = argparse.ArgumentParser(
        prog="wiji",
        description="""wiji is an async distributed task queue.
                example usage:
                wiji-cli \
                --app dotted.path.to.a.wiji.app.App.class.instance
                """,
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=wiji.__version__.about["__version__"]),
        help="The currently installed wiji version.",
    )
    parser.add_argument(
        "--app",
        required=True,
        help="The dotted path to a python file conatining a `wiji.app.App` instance. \
        eg: --app dotted.path.to.a.wiji.app.App.class.instance",
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
        wiji-cli --app dotted.path.to.a.wiji.app.App.class.instance
    """
    worker_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=17))
    logger = wiji.logger.SimpleLogger("wiji.cli")
    logger.log(logging.INFO, {"event": "wiji.cli.main", "stage": "start", "worker_id": worker_id})
    try:
        parser = make_parser()
        args = parser.parse_args()

        app = args.app
        dry_run = args.dry_run
        if dry_run:
            logger.log(
                logging.WARNING,
                "\n\n\t {} \n\n".format(
                    "Wiji: Caution; You have activated dry-run, wiji may not behave correctly."
                ),
            )

        app_instance = utils.load.load_class(app)
        if not isinstance(app_instance, wiji.app.App):
            err = ValueError(
                """`app_instance` should be of type:: `wiji.app.App` You entered: {0}""".format(
                    type(app_instance)
                )
            )
            logger.log(logging.ERROR, {"event": "wiji.cli.main", "stage": "end", "error": str(err)})
            sys.exit(77)

        if dry_run:
            logger.log(
                logging.INFO, {"event": "wiji.cli.main", "stage": "end", "state": "dry_run end"}
            )
            return

        asyncio_debug = False
        if os.environ.get("WIJI_DEBUG", None):
            asyncio_debug = True
        asyncio.run(async_main(logger=logger, app_instance=app_instance), debug=asyncio_debug)
    except Exception as e:
        logger.log(logging.ERROR, {"event": "wiji.cli.main", "stage": "end", "error": str(e)})
        sys.exit(77)
    finally:
        logger.log(logging.INFO, {"event": "wiji.cli.main", "stage": "end"})


async def async_main(logger: wiji.logger.BaseLogger, app_instance: wiji.app.App) -> None:
    """
    (i)   set signal handlers.
    (ii)  consume tasks.
    (iii) continuously produce watchdog tasks.
    """
    watchdog_worker = wiji.Worker(
        the_task=wiji.task.WatchDogTask,
        use_watchdog=True,
        watchdog_duration=app_instance.watchdog_duration,
    )
    workers = [watchdog_worker]
    watch_dog_producer = [utils._producer.produce_tasks_continously(task=wiji.task.WatchDogTask)]

    _queue_names: typing.List[str] = []
    for task_class in app_instance.task_classes:
        if task_class.queue_name in _queue_names:
            # queue names should be unique
            raise ValueError(
                "There already exists a task with queue_name: {0}".format(task_class.queue_name)
            )
        _queue_names.append(task_class.queue_name)

        task = task_class()
        _worker = wiji.Worker(the_task=task)
        workers.append(_worker)

    del _queue_names
    consumers = []
    for i in workers:
        consumers.append(i.consume_tasks())

    gather_tasks = asyncio.gather(
        *consumers, *watch_dog_producer, utils.sig._signal_handling(logger=logger, workers=workers)
    )
    await gather_tasks


if __name__ == "__main__":
    main()

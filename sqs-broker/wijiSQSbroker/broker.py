import wiji
import typing
import asyncio
import logging
import datetime
import functools
import concurrent
import botocore.session


# See SQS limits: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-limits.html
# TODO: we need to add this limits as validations to this broker


# TODO: add batching
# TODO: add compression
# TODO: add long-polling; https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html


class SqsBroker(wiji.broker.BaseBroker):
    """
    """

    def __init__(
        self,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        MessageRetentionPeriod: int = 345_600,
        MaximumMessageSize: int = 262_144,
        ReceiveMessageWaitTimeSeconds: int = 0,
        VisibilityTimeout: int = 30,
        DelaySeconds: int = 0,
        loglevel: str = "DEBUG",
        log_handler: typing.Union[None, wiji.logger.BaseLogger] = None,
    ) -> None:
        self._validate_args(
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            MessageRetentionPeriod=MessageRetentionPeriod,
            MaximumMessageSize=MaximumMessageSize,
            ReceiveMessageWaitTimeSeconds=ReceiveMessageWaitTimeSeconds,
            VisibilityTimeout=VisibilityTimeout,
            DelaySeconds=DelaySeconds,
            loglevel=loglevel,
            log_handler=log_handler,
        )

        self.loglevel = loglevel.upper()
        self.logger = log_handler
        if not self.logger:
            self.logger = wiji.logger.SimpleLogger("wiji.SqsBroker")
        self.logger.bind(level=self.loglevel, log_metadata={})
        self._sanity_check_logger(event="sqsBroker_sanity_check_logger")

        self.MessageRetentionPeriod = MessageRetentionPeriod
        self.MaximumMessageSize = MaximumMessageSize
        self.ReceiveMessageWaitTimeSeconds = ReceiveMessageWaitTimeSeconds
        self.VisibilityTimeout = VisibilityTimeout
        self.DelaySeconds = DelaySeconds

        self.QueueUrl: typing.Union[None, str] = None
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.session = botocore.session.Session()
        self.client = self.session.create_client(
            self,
            service_name="sqs",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            use_ssl=True,
        )

        self.task_receipt: str = {}
        self._thread_name_prefix: str = "wiji-SqsBroker-thread-pool"
        self.loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()

    def _validate_task_args(
        self,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        MessageRetentionPeriod: int,
        MaximumMessageSize: int,
        ReceiveMessageWaitTimeSeconds: int,
        VisibilityTimeout: int,
        DelaySeconds: int,
        loglevel: str,
        log_handler: typing.Union[None, wiji.logger.BaseLogger],
    ) -> None:
        if not isinstance(region_name, str):
            raise ValueError(
                """`region_name` should be of type:: `str` You entered: {0}""".format(
                    type(region_name)
                )
            )
        if not isinstance(aws_access_key_id, str):
            raise ValueError(
                """`aws_access_key_id` should be of type:: `str` You entered: {0}""".format(
                    type(aws_access_key_id)
                )
            )
        if not isinstance(aws_secret_access_key, str):
            raise ValueError(
                """`aws_secret_access_key` should be of type:: `str` You entered: {0}""".format(
                    type(aws_secret_access_key)
                )
            )
        if loglevel.upper() not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(
                """`loglevel` should be one of; 'DEBUG', 'INFO', 'WARNING', 'ERROR' or 'CRITICAL'. You entered: {0}""".format(
                    loglevel
                )
            )
        if not isinstance(log_handler, (type(None), wiji.logger.BaseLogger)):
            raise ValueError(
                """`log_handler` should be of type:: `None` or `wiji.logger.BaseLogger` You entered: {0}""".format(
                    type(log_handler)
                )
            )

        self._validate_sqs(
            MessageRetentionPeriod=MessageRetentionPeriod,
            MaximumMessageSize=MaximumMessageSize,
            ReceiveMessageWaitTimeSeconds=ReceiveMessageWaitTimeSeconds,
            VisibilityTimeout=VisibilityTimeout,
            DelaySeconds=DelaySeconds,
        )

    def _validate_sqs(
        self,
        MessageRetentionPeriod: int,
        MaximumMessageSize: int,
        ReceiveMessageWaitTimeSeconds: int,
        VisibilityTimeout: int,
        DelaySeconds: int,
    ):
        if not isinstance(MessageRetentionPeriod, int):
            raise ValueError(
                """`MessageRetentionPeriod` should be of type:: `int` You entered: {0}""".format(
                    type(MessageRetentionPeriod)
                )
            )
        if MessageRetentionPeriod < 60:
            raise ValueError("""`MessageRetentionPeriod` should not be less than 60 seconds""")
        elif MessageRetentionPeriod > 1_209_600:
            raise ValueError(
                """`MessageRetentionPeriod` should not be greater than 1_209_600 seconds"""
            )

        if not isinstance(MaximumMessageSize, int):
            raise ValueError(
                """`MaximumMessageSize` should be of type:: `int` You entered: {0}""".format(
                    type(MaximumMessageSize)
                )
            )
        if MaximumMessageSize < 1024:
            raise ValueError("""`MaximumMessageSize` should not be less than 1024 bytes""")
        elif MaximumMessageSize > 262_144:
            raise ValueError("""`MaximumMessageSize` should not be greater than 262_144 bytes""")

        if not isinstance(ReceiveMessageWaitTimeSeconds, int):
            raise ValueError(
                """`ReceiveMessageWaitTimeSeconds` should be of type:: `int` You entered: {0}""".format(
                    type(ReceiveMessageWaitTimeSeconds)
                )
            )
        if ReceiveMessageWaitTimeSeconds < 0:
            raise ValueError(
                """`ReceiveMessageWaitTimeSeconds` should not be less than 0 seconds"""
            )
        elif ReceiveMessageWaitTimeSeconds > 20:
            raise ValueError(
                """`ReceiveMessageWaitTimeSeconds` should not be greater than 20 seconds"""
            )

        if not isinstance(VisibilityTimeout, int):
            raise ValueError(
                """`VisibilityTimeout` should be of type:: `int` You entered: {0}""".format(
                    type(VisibilityTimeout)
                )
            )
        if VisibilityTimeout < 0:
            raise ValueError("""`VisibilityTimeout` should not be less than 0 seconds""")
        elif VisibilityTimeout > 43200:
            raise ValueError("""`VisibilityTimeout` should not be greater than 43200 seconds""")

        if not isinstance(DelaySeconds, int):
            raise ValueError(
                """`DelaySeconds` should be of type:: `int` You entered: {0}""".format(
                    type(DelaySeconds)
                )
            )
        if DelaySeconds < 0:
            raise ValueError("""`DelaySeconds` should not be less than 0 seconds""")
        elif DelaySeconds > 900:
            raise ValueError("""`DelaySeconds` should not be greater than 900 seconds""")

    def _sanity_check_logger(self, event: str) -> None:
        """
        Called when we want to make sure the supplied logger can log.
        """
        try:
            assert isinstance(self.logger, wiji.logger.BaseLogger)  # make mypy happy
            self.logger.log(logging.DEBUG, {"event": event})
        except Exception as e:
            raise e

    async def check(self, queue_name: str) -> None:
        """
        - If you provide the name of an existing queue along with the exact names and values of all the queue's attributes,
          CreateQueue returns the queue URL for the existing queue.
        - If the queue name, attribute names, or attribute values don't match an existing queue, CreateQueue returns an error.
        - A queue name can have up to 80 characters.
          Valid values: alphanumeric characters, hyphens (- ), and underscores (_ ).
        """
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:
            await self.loop.run_in_executor(
                executor, functools.partial(self.blocking_check, queue_name=queue_name)
            )

    def blocking_check(self, queue_name: str) -> None:
        try:
            response = self.client.create_queue(
                QueueName=queue_name,
                Attributes={
                    "MessageRetentionPeriod": self.MessageRetentionPeriod,
                    "MaximumMessageSize": self.MaximumMessageSize,
                    "ReceiveMessageWaitTimeSeconds": self.ReceiveMessageWaitTimeSeconds,
                    "VisibilityTimeout": self.VisibilityTimeout,
                    "DelaySeconds": self.DelaySeconds,
                },
            )
            QueueUrl = response["QueueUrl"]
            self.QueueUrl = QueueUrl
        except Exception as e:
            raise e

    async def enqueue(
        self, item: str, queue_name: str, task_options: wiji.task.TaskOptions
    ) -> None:
        """
        """
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:
            await self.loop.run_in_executor(
                executor,
                functools.partial(
                    self.blocking_enqueue,
                    item=item,
                    queue_name=queue_name,
                    task_options=task_options,
                ),
            )

    def blocking_enqueue(
        self, item: str, queue_name: str, task_options: wiji.task.TaskOptions
    ) -> None:
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        task_eta = task_options.eta
        task_eta = wiji.protocol.Protocol._from_isoformat(task_eta)

        delay = self.DelaySeconds
        if task_eta > now:
            diff = task_eta - now
            delay = diff.seconds
            if delay > 900:
                delay = 900

        response = self.client.send_message(
            QueueUrl=self.QueueUrl,
            MessageBody=item,
            DelaySeconds=delay,
            MessageAttributes={
                "string": {
                    "user": "wiji.SqsBroker",
                    "task_eta": task_options.eta,
                    "task_id": task_options.task_id,
                    "task_hook_metadata": task_options.hook_metadata,
                }
            },
        )
        _ = response["MD5OfMessageBody"]
        _ = response["MD5OfMessageAttributes"]
        _ = response["MessageId"]
        _ = response["SequenceNumber"]

    async def dequeue(self, queue_name: str) -> str:
        """
        """
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:
            while True:
                item = await self.loop.run_in_executor(
                    executor, functools.partial(self.blocking_dequeue, queue_name=queue_name)
                )
                if item:
                    return item
                else:
                    await asyncio.sleep(5)

    def blocking_dequeue(self, queue_name: str) -> str:
        response = self.client.receive_message(
            QueueUrl=self.QueueUrl,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=1,
            # VisibilityTimeout: The duration (in seconds) that the received messages are hidden
            # from subsequent retrieve requests after being retrieved by a ReceiveMessage request.
            VisibilityTimeout=self.VisibilityTimeout,
            # WaitTimeSeconds: The duration (in seconds) for which the call waits for a message to arrive in the queue before returning.
            # If no messages are available and the wait time expires, the call returns successfully with an empty list of messages.
            WaitTimeSeconds=5,
        )

        if len(response["Messages"]) >= 1:
            ReceiptHandle = response["Messages"][0]["ReceiptHandle"]
            MessageAttributes = response["Messages"][0]["MessageAttributes"]
            task_id = MessageAttributes["task_id"]
            self.task_receipt[task_id] = ReceiptHandle

            item = response["Messages"][0]["Body"]
            return item
        else:
            return None

    async def done(
        self,
        item: str,
        queue_name: str,
        task_options: wiji.task.TaskOptions,
        state: wiji.task.TaskState,
    ) -> None:
        """
        """
        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix=self._thread_name_prefix
        ) as executor:
            await self.loop.run_in_executor(
                executor,
                functools.partial(
                    self.blocking_done,
                    item=item,
                    queue_name=queue_name,
                    task_options=task_options,
                    state=state,
                ),
            )

    def blocking_done(
        self,
        item: str,
        queue_name: str,
        task_options: wiji.task.TaskOptions,
        state: wiji.task.TaskState,
    ) -> None:
        ReceiptHandle = self.task_receipt.pop(task_options.task_id, None)
        if ReceiptHandle:
            response = self.client.delete_message(
                QueueUrl=self.QueueUrl, ReceiptHandle=ReceiptHandle
            )

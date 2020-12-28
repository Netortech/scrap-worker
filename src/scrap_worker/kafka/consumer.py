import confluent_kafka
import logging

from confluent_kafka import Consumer
from typing import Any, List, Optional, Union


_logger = logging.getLogger(__name__)

DEFAULT_QUEUED_MAX_MESSAGES_KBYTES = 10000


class KafkaMessageWrapper:
    def __init__(self, raw_msg: confluent_kafka.cimpl.Message):
        assert raw_msg is not None, "Raw message cannot be None."
        self._raw_msg = raw_msg

    def value(self) -> Union[str, bytes, bytearray]:
        return self._raw_msg.value()

    def topic(self) -> str:
        return self._raw_msg.topic()

    def partition(self) -> int:
        return self._raw_msg.partition()

    def offset(self) -> int:
        return self._raw_msg.offset()


class KafkaConsumer:
    """
    A wrapper for a Kafka Consumer. Currently uses the confluent kafka implementation.

    Future Note:
    As this project is supposed to run on Raspberry Pi (which I have not yet had time to test)
    we may need to switch this to use the python kafka library:

    https://github.com/dpkp/kafka-python

    As this is implemented natively in Python it may represent a lower hurdle for getting
    this application running on a pi.
    """

    def __init__(
        self,
        id: str,
        topics: List[str],
        brokers: str,
        offset_reset_to_latest: bool = True,
    ):
        consumer_config = {
            "bootstrap.servers": brokers,
            "group.id": "scrapworker-" + id,
            "auto.offset.reset": "latest" if offset_reset_to_latest else "earliest",
            "enable.auto.commit": False,
            "queued.max.messages.kbytes": DEFAULT_QUEUED_MAX_MESSAGES_KBYTES,
        }

        _logger.debug(f"Kafka Consumer Config: {consumer_config}")

        self._consumer = Consumer(consumer_config)

        # subscribe to topic
        self._debug(f"Subscribing to topics: {str(topics)}")
        self._consumer.subscribe(topics)
        self._assignment: List[Any] = []

    def _debug(self, msg: str):
        _logger.debug(f"{msg}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._consumer.close()

    def commit(self):
        self._consumer.commit()

    def get_assignment(self):
        return self._consumer.assignment()

    def consume(
        self, ms: int = 500, wait_for_assignment: bool = False
    ) -> Optional[KafkaMessageWrapper]:
        """
        Consumes a message from the kafka topic supplied in the constructor.

        optional:
        ms: The timeout, in seconds, to wait for a message. None is returned if
            no messages are consumed.
        wait_for_assignment: Will block until the consumer has established a
            connection with the broker and received at least one partition/offset
            assignment.
        """
        msg = self._consumer.poll(ms / 1000)

        # The confluent consumer doesn't block on the constructor or poll (with timeout)
        # while it's connecting to the broker. In some cases, like where we're both a
        # producer and a consumer, we need to know that the consumer has been assigned
        # a partition and offset so we can begin producing.
        # The loop also breaks if msg is not None because if we got a message we
        # must have an assignment.
        # TODO: Need to set a timeout for this as well.
        while wait_for_assignment and msg is None and len(self._assignment) == 0:
            self._debug("Waiting for topic assignment.")
            self._assignment = self._consumer.assignment()
            msg = self._consumer.poll(ms / 1000)

        return None if msg is None else KafkaMessageWrapper(raw_msg=msg)

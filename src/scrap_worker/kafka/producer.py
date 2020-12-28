import logging

from confluent_kafka import Producer
from confluent_kafka.cimpl import KafkaException

from pydantic import BaseModel

_logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    A wrapper for a Kafka Producer. Currently uses the confluent kafka implementation.

    Future Note:
    As this project is supposed to run on Raspberry Pi (which I have not yet had time to test)
    we may need to switch this to use the python kafka library:

    https://github.com/dpkp/kafka-python

    As this is implemented natively in Python it may represent a lower hurdle for getting
    this application running on a pi.
    """

    def __init__(self, broker: str, topic: str = None):
        self._topic = topic

        producer_config = {
            "bootstrap.servers": broker,
            "default.topic.config": {"acks": 1},
        }

        self._debug(f"Kafka producer config: {producer_config}. Topic: {self._topic}")

        self._producer = Producer(producer_config)

    def _debug(self, msg: str):
        _logger.debug(f"{msg}")

    @staticmethod
    def delivery_report(err: str, msg):
        if err is not None:
            _logger.error(f"Delivery failed for data {msg.value()} error: {err}")
        else:
            _logger.debug(
                f"Data {msg.value()} delivered to: {msg.topic()}[{msg.partition()}]"
            )

    def produce(self, message: BaseModel, topic: str = None, debug_mode: bool = False):
        """
        Produce a message to a kafka topic.

        required:
        message:   Must be an instance of `pydantic.BaseModel`. Will automatically convert
                    to JSON. Binary serialization is not supported at this time but is
                    targetted for future work.
        optional:
        topic:      The topic to produce to. If None is specified the topic provided in the
                    constructor is used.
        debug_mode: If set to True will output errors and debug information to logger.
        """
        topic = topic if topic is not None else self._topic
        try:
            self._producer.produce(
                topic,
                message.json().encode("utf-8"),
                callback=KafkaProducer.delivery_report if debug_mode else None,
            )
        except KafkaException:
            _logger.exception("Unable to produce message to kafka")
        if debug_mode:
            self._producer.poll()

    def flush(self):
        self._producer.flush()

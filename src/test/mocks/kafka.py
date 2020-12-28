from pydantic import BaseModel
from queue import Queue, Empty
from typing import List, Optional

from ...scrap_worker.kafka import KafkaConsumer, KafkaMessageWrapper, KafkaProducer


class KafkaMessageWrapperMock(KafkaMessageWrapper):
    def __init__(
        self,
        value: str,
        topic: str = "TOPIC-NOT-SET",
        partition: int = -1,
        offset: int = -1,
    ):
        self._value = value
        self._topic = topic
        self._offset = offset
        self._partition = partition

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class KafkaProducerBaseMock(KafkaProducer):
    def __init__(self, output: Queue):
        self.__TESTING_output = output

    def produce(self, message: BaseModel, topic: str = None, debug_mode: bool = False):
        self.__TESTING_output.put(
            KafkaMessageWrapperMock(message.json().encode("utf-8"))
        )

    def flush(self):
        pass


class KafkaConsumerBaseMock(KafkaConsumer):
    def __init__(self, input: Queue):
        self.__TESTING_input = input

    def consume(
        self, ms: int = 500, wait_for_assignment: bool = False
    ) -> Optional[KafkaMessageWrapper]:
        try:
            result = self.__TESTING_input.get_nowait()
        except Empty:
            result = None
        assert result is None or isinstance(
            result, KafkaMessageWrapper
        ), f"Testing error. Queue must only contain objects of type {KafkaMessageWrapper.__name__}."
        return result

    def commit(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass

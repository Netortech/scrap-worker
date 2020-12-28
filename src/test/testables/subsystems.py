from __future__ import annotations

from ...scrap_worker.subsystems import (
    HeartbeatSubsystem,
    ElectionSubsystem,
    QuellUprisingSubsystem,
    UnionAttendanceSubsystem,
    UnionLeadershipSubsystem,
    UnionMembershipSubsystem,
    WorkloadPreprocessorSubsystem,
)
from ..mocks.kafka import KafkaProducerBaseMock, KafkaConsumerBaseMock


class ElectionSubsystemTestable(ElectionSubsystem):
    def _set_mock_producer(
        self, producer: KafkaProducerBaseMock
    ) -> ElectionSubsystemTestable:
        self.__TESTING_producer = producer
        return self

    def _configure_producer(self) -> KafkaProducerBaseMock:
        return self.__TESTING_producer

    def _set_mock_consumer(
        self, consumer: KafkaConsumerBaseMock
    ) -> ElectionSubsystemTestable:
        self.__TESTING_consumer = consumer
        return self

    def _configure_consumer(self) -> KafkaConsumerBaseMock:
        return self.__TESTING_consumer


class HeartbeatSubsystemTestable(HeartbeatSubsystem):
    def _set_mock_producer(
        self, producer: KafkaProducerBaseMock
    ) -> HeartbeatSubsystemTestable:
        self.__TESTING_producer = producer

    def _configure_producer(self) -> KafkaProducerBaseMock:
        return self.__TESTING_producer


class QuellUprisingSubsystemTestable(QuellUprisingSubsystem):
    def _set_mock_producer(
        self, producer: KafkaProducerBaseMock
    ) -> QuellUprisingSubsystemTestable:
        self.__TESTING_producer = producer
        return self

    def _configure_producer(self) -> KafkaProducerBaseMock:
        return self.__TESTING_producer

    def _set_mock_consumer(
        self, consumer: KafkaConsumerBaseMock
    ) -> QuellUprisingSubsystemTestable:
        self.__TESTING_consumer = consumer
        return self

    def _configure_consumer(self) -> KafkaConsumerBaseMock:
        return self.__TESTING_consumer


class UnionAttendanceSubsystemTestable(UnionAttendanceSubsystem):
    def _set_mock_consumer(
        self, consumer: KafkaConsumerBaseMock
    ) -> UnionAttendanceSubsystemTestable:
        self.__TESTING_consumer = consumer
        return self

    def _configure_consumer(self) -> KafkaConsumerBaseMock:
        return self.__TESTING_consumer


class UnionMembershipSubsystemTestable(UnionMembershipSubsystem):
    def _set_mock_consumer(
        self, consumer: KafkaConsumerBaseMock
    ) -> UnionMembershipSubsystemTestable:
        self.__TESTING_consumer = consumer
        return self

    def _configure_consumer(self) -> KafkaConsumerBaseMock:
        return self.__TESTING_consumer

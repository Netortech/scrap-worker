from pydantic import BaseModel
from queue import Queue
from threading import Thread
from typing import List

from ..scrap_worker.app_state_machine import (
    ScrapWorkerSubsystemConfigurations,
)
from ..scrap_worker.state_machine.models import StateFlag, TimeoutFlag
from ..scrap_worker.models.event_models import CandidacyEvent
from .mocks.kafka import (
    KafkaProducerBaseMock,
    KafkaConsumerBaseMock,
    KafkaMessageWrapperMock,
)
from .testables.subsystems import QuellUprisingSubsystemTestable

worker_id: str = "00000000-0000-0000-0000-000000000000"

messages: Queue = Queue(maxsize=100)
messages.put(
    KafkaMessageWrapperMock(
        CandidacyEvent(
            worker_id=worker_id,
            ranking=1,
        )
        .json()
        .encode("utf-8")
    )
)


def test_quell_uprising_subsystem_exit_flag():
    exit_flag = StateFlag("test_exit_flag")
    timeout = TimeoutFlag(3000, "Test timeout")

    c = ScrapWorkerSubsystemConfigurations(
        global_exit_flag=exit_flag
    ).quell_uprising_subsystem_configuration
    thread = Thread(
        target=QuellUprisingSubsystemTestable(c)
        ._set_mock_producer(KafkaProducerBaseMock(output=messages))
        ._set_mock_consumer(KafkaConsumerBaseMock(input=messages))
        .start
    )
    thread.start()

    while not c.started_flag.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem to mark itself as started."

    ready_to_exit = TimeoutFlag(500, "Letting subsystem run for a bit.")
    while not ready_to_exit.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out while letting subsystem run. (May mean start up took longer than it should.)"

    assert (
        not c.exited_flag.get_value()
    ), "Subsystem exited before the global exit flag was set."

    exit_flag.set_value()

    while not c.exited_flag.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem to mark itself as exited"
    while thread.is_alive():
        thread.join(10)
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem thread to die."

    assert not c.exception_flag.get_value(), "Subsystem threw an unhandled exception."
    assert messages.empty(), "Subsystem did not consume any messages."


def test_quell_uprising_subsystem_exception_flag_on_consume():
    exit_flag = StateFlag("test_exit_flag")
    should_throw = StateFlag("should_throw")
    timeout = TimeoutFlag(3000, "Test timeout")

    class KafkaConsumerMock(KafkaConsumerBaseMock):
        def consume(self, ms: int = 500, wait_for_assignment: bool = False):
            super(KafkaConsumerMock, self).consume(
                ms=ms, wait_for_assignment=wait_for_assignment
            )
            if should_throw.get_value():
                raise Exception("Simulate consumer exception")

    c = ScrapWorkerSubsystemConfigurations(
        global_exit_flag=exit_flag
    ).quell_uprising_subsystem_configuration
    thread = Thread(
        target=QuellUprisingSubsystemTestable(c)
        ._set_mock_producer(KafkaProducerBaseMock(output=messages))
        ._set_mock_consumer(KafkaConsumerMock(input=messages))
        .start
    )
    thread.start()

    while not c.started_flag.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem to mark itself as started."

    ready_to_throw = TimeoutFlag(500, "Letting subsystem run for a bit.")
    while not ready_to_throw.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out while letting subsystem run. (May mean start up took too long.)"

    should_throw.set_value()
    while not c.exception_flag.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for exception to be thrown."
    while not c.exited_flag.get_value():
        assert (
            not timeout.get_value()
        ), "Timed out waiting for subsystem to exit after throwing the exception."
    while thread.is_alive():
        thread.join(10)
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem thread to die."

    assert messages.empty(), "Subsystem did not consume any messages."

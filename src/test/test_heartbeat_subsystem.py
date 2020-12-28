from pydantic import BaseModel
from queue import Queue
from threading import Thread
from typing import List

from ..scrap_worker.app_state_machine import (
    ScrapWorkerSubsystemConfigurations,
)
from ..scrap_worker.state_machine.models import StateFlag, TimeoutFlag
from .mocks.kafka import KafkaProducerBaseMock
from .testables.subsystems import HeartbeatSubsystemTestable

messages: Queue = Queue(maxsize=100)


def test_heartbeat_subsystem_exit_flag():
    exit_flag = StateFlag("test_exit_flag")
    all_configs = ScrapWorkerSubsystemConfigurations(global_exit_flag=exit_flag)
    c = all_configs.heartbeat_subsystem_configuration
    c.heartbeat_delay_ms = 100
    subsystem = HeartbeatSubsystemTestable(c)
    mock_producer = KafkaProducerBaseMock(output=messages)
    subsystem._set_mock_producer(mock_producer)
    thread = Thread(target=subsystem.start)
    timeout = TimeoutFlag(3000, "Test timeout")

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
    assert messages.qsize() > 0, "Subsystem did not emit any messages."


def test_heartbeat_subsystem_exception_flag():
    should_throw = StateFlag("should_throw")

    class KafkaProducerExceptionMock(KafkaProducerBaseMock):
        def produce(
            self, message: BaseModel, topic: str = None, debug_mode: bool = False
        ):
            super(KafkaProducerExceptionMock, self).produce(
                message=message, topic=topic, debug_mode=debug_mode
            )
            if should_throw.get_value():
                raise Exception("Simulate producer exception")

    exit_flag = StateFlag("test_exit_flag")
    all_configs = ScrapWorkerSubsystemConfigurations(global_exit_flag=exit_flag)
    c = all_configs.heartbeat_subsystem_configuration
    c.heartbeat_delay_ms = 100
    subsystem = HeartbeatSubsystemTestable(c)
    mock_producer = KafkaProducerExceptionMock(output=messages)
    subsystem._set_mock_producer(mock_producer)
    thread = Thread(target=subsystem.start)
    timeout = TimeoutFlag(3000, "Test timeout")

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

    assert messages.qsize() > 0, "Subsystem did not emit any messages."

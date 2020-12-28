from pydantic import BaseModel
from queue import Queue
from threading import Thread
from typing import List

from ..scrap_worker.models.event_models import LeaderInstructionEvent
from ..scrap_worker.app_state_machine import (
    ScrapWorkerSubsystemConfigurations,
)
from ..scrap_worker.state_machine.models import StateFlag, TimeoutFlag
from .mocks.kafka import KafkaConsumerBaseMock, KafkaMessageWrapperMock
from .testables.subsystems import UnionMembershipSubsystemTestable

messages: Queue = Queue(maxsize=100)
leader_uid: str = "00000000-0000-0000-0000-000000000000"
non_leader_uid: str = "00000000-0000-0000-0000-000000000001"


def test_union_membership_subsystem_exit_flag():
    exit_flag = StateFlag("test_exit_flag")
    all_configs = ScrapWorkerSubsystemConfigurations(global_exit_flag=exit_flag)
    c = all_configs.union_membership_subsystem_configuration
    c.leadership_ttl_ms = 10000  # 10 seconds
    subsystem = UnionMembershipSubsystemTestable(c)
    mock_consumer = KafkaConsumerBaseMock(input=messages)
    subsystem._set_mock_consumer(mock_consumer)
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


def test_union_membership_subsystem_exception_flag():
    should_throw = StateFlag("should_throw")

    class KafkaConsumerMock(KafkaConsumerBaseMock):
        def consume(self, ms: int = 500, wait_for_assignment: bool = False):
            super(KafkaConsumerMock, self).consume(
                ms=ms, wait_for_assignment=wait_for_assignment
            )
            if should_throw.get_value():
                raise Exception("Simulate consumer exception")

    exit_flag = StateFlag("test_exit_flag")
    all_configs = ScrapWorkerSubsystemConfigurations(global_exit_flag=exit_flag)
    c = all_configs.union_membership_subsystem_configuration
    c.leadership_ttl_ms = 10000  # 10 seconds
    subsystem = UnionMembershipSubsystemTestable(c)
    mock_consumer = KafkaConsumerMock(input=messages)
    subsystem._set_mock_consumer(mock_consumer)
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
        ), "Test timed out while letting subsystem run. (May mean start up took longer than it should.)"

    assert (
        not c.exited_flag.get_value()
    ), "Subsystem exited before the global exit flag was set."

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


def test_union_membership_subsystem_lost_leader_flag():
    exit_flag = StateFlag("test_exit_flag")
    all_configs = ScrapWorkerSubsystemConfigurations(global_exit_flag=exit_flag)
    c = all_configs.union_membership_subsystem_configuration
    c.leadership_ttl_ms = 500  # 10 seconds
    subsystem = UnionMembershipSubsystemTestable(c)
    mock_consumer = KafkaConsumerBaseMock(input=messages)
    subsystem._set_mock_consumer(mock_consumer)
    thread = Thread(target=subsystem.start)
    timeout = TimeoutFlag(3000, "Test timeout")

    thread.start()
    while not c.started_flag.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem to mark itself as started."

    messages.put(
        KafkaMessageWrapperMock(
            LeaderInstructionEvent(
                worker_id=leader_uid,
                op_code="NOOP",
            )
            .json()
            .encode("utf-8")
        )
    )
    messages.put(
        KafkaMessageWrapperMock(
            LeaderInstructionEvent(
                worker_id=non_leader_uid,
                op_code="NOOP",
            )
            .json()
            .encode("utf-8")
        )
    )
    messages.put(KafkaMessageWrapperMock("invalid message"))

    while messages.qsize() > 0:
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem to consume leader NOOP message."

    while c.leader_id is None:
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem to set leader id config."

    assert (
        str(c.leader_id) == leader_uid
    ), "Leader id config does not match provided test uid in leader NOOP message."

    while not c.lost_leader_flag.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem to mark leader as lost."
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

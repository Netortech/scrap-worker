from pydantic import BaseModel
from queue import Queue
from threading import Thread
from typing import List

from ..scrap_worker.models.event_models import HeartbeatEvent
from ..scrap_worker.app_state_machine import (
    ScrapWorkerSubsystemConfigurations,
)
from ..scrap_worker.state_machine.models import StateFlag, TimeoutFlag
from .mocks.kafka import KafkaConsumerBaseMock, KafkaMessageWrapperMock
from .testables.subsystems import UnionAttendanceSubsystemTestable

messages: Queue = Queue(maxsize=100)
member_uid: str = "00000000-0000-0000-0000-000000000000"
non_member_uid: str = "00000000-0000-0000-0000-000000000001"


def test_union_attendance_subsystem_exit_flag():
    exit_flag = StateFlag("test_exit_flag")
    timeout = TimeoutFlag(3000, "Test timeout")

    c = ScrapWorkerSubsystemConfigurations(
        global_exit_flag=exit_flag
    ).union_attendance_subsystem_configuration
    thread = Thread(
        target=UnionAttendanceSubsystemTestable(c)
        ._set_mock_consumer(KafkaConsumerBaseMock(input=messages))
        .start
    )
    thread.start()

    # Simulating the leader's own heartbeat
    messages.put(
        KafkaMessageWrapperMock(
            HeartbeatEvent(
                worker_id=str(c.worker_id),
                count=1,
            )
            .json()
            .encode("utf-8")
        )
    )
    # Simulating a member heartbeat
    messages.put(
        KafkaMessageWrapperMock(
            HeartbeatEvent(
                worker_id=member_uid,
                count=1,
                leader_id=str(c.worker_id),
            )
            .json()
            .encode("utf-8")
        )
    )
    # Simulating a non-member heartbeat
    messages.put(
        KafkaMessageWrapperMock(
            HeartbeatEvent(
                worker_id=non_member_uid,
                count=1,
            )
            .json()
            .encode("utf-8")
        )
    )
    # Simulating a non-member heartbeat following another leader
    messages.put(
        KafkaMessageWrapperMock(
            HeartbeatEvent(
                worker_id=non_member_uid,
                count=1,
                leader_id=member_uid,
            )
            .json()
            .encode("utf-8")
        )
    )
    messages.put(KafkaMessageWrapperMock("invalid message."))

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
    assert messages.empty(), "Subsystem did not consume all messages."
    assert (
        not c.union_membership_changed_event_queue.empty()
    ), "Attendance subsystem did not publish union membership change event."


def test_union_membership_subsystem_exception_flag():
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
    ).union_attendance_subsystem_configuration
    thread = Thread(
        target=UnionAttendanceSubsystemTestable(c)
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

from pydantic import BaseModel
from queue import Queue
from threading import Thread
from typing import List

from ..scrap_worker.models.event_models import CandidacyEvent
from ..scrap_worker.app_state_machine import ScrapWorkerSubsystemConfigurations
from ..scrap_worker.state_machine.models import StateFlag, TimeoutFlag
from .mocks.kafka import (
    KafkaConsumerBaseMock,
    KafkaMessageWrapperMock,
    KafkaProducerBaseMock,
)
from .testables.subsystems import ElectionSubsystemTestable

messages: Queue = Queue(maxsize=100)
leader_uid: str = "00000000-0000-0000-0000-000000000000"


def test_election_subsystem_exit_flag():
    exit_flag = StateFlag("test_exit_flag")
    all_configs = ScrapWorkerSubsystemConfigurations(global_exit_flag=exit_flag)
    c = all_configs.election_subsystem_configuration
    c.ranking = 1000
    c.countdown_start = 10
    c.countdown_delay_ms = 500
    c.campaign_ttl_ms = 5000
    subsystem = ElectionSubsystemTestable(c)
    mock_producer = KafkaProducerBaseMock(output=messages)
    subsystem._set_mock_producer(mock_producer)
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


def test_election_subsystem_elected_new_leader():
    voted_for_us = StateFlag("voted_for_us")
    exit_flag = StateFlag("test_exit_flag")
    all_configs = ScrapWorkerSubsystemConfigurations(global_exit_flag=exit_flag)
    c = all_configs.election_subsystem_configuration

    class KafkaProducerDetectVoteMock(KafkaProducerBaseMock):
        def produce(
            self, message: BaseModel, topic: str = None, debug_mode: bool = False
        ):
            super(KafkaProducerDetectVoteMock, self).produce(
                message=message, topic=topic, debug_mode=debug_mode
            )
            assert isinstance(
                message, CandidacyEvent
            ), f"Election subsystem must only produce messages of type {CandidacyEvent.__name__}"
            candidacy: CandidacyEvent = message
            assert candidacy.worker_id == str(
                c.worker_id
            ), "Expected configured worker id to be specified in broadcasted candidacy."
            if candidacy.withdrawn_and_nominating_worker_id == leader_uid:
                voted_for_us.set_value()

    initial_ranking = 1000
    c.ranking = initial_ranking
    c.countdown_start = 10
    c.countdown_delay_ms = 500
    c.campaign_ttl_ms = 5000
    subsystem = ElectionSubsystemTestable(c)
    mock_producer = KafkaProducerDetectVoteMock(output=messages)
    subsystem._set_mock_producer(mock_producer)
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

    messages.put(
        KafkaMessageWrapperMock(
            CandidacyEvent(
                worker_id=leader_uid,
                # Don't try to test ties here.
                ranking=initial_ranking + 1,
            )
            .json()
            .encode("utf-8")
        )
    )

    while not voted_for_us.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for election subsystem to vote for us."

    messages.put(
        KafkaMessageWrapperMock(
            CandidacyEvent(
                worker_id=leader_uid,
                ranking=initial_ranking + 1,
                acceptance_countdown=0,
            )
            .json()
            .encode("utf-8")
        )
    )

    while not c.elected_new_leader_flag.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem accept us as a leader."
    while not c.exited_flag.get_value():
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem to mark itself as exited"
    while thread.is_alive():
        thread.join(10)
        assert (
            not timeout.get_value()
        ), "Test timed out waiting for subsystem thread to die."
    assert (
        c.winning_candidacy is not None
    ), "Election subsystem did not provide the winning candidacy."
    assert (
        c.winning_candidacy.worker_id == leader_uid
    ), "Election subsystem did not provide our candidacy as the winning candidacy."
    assert not c.exception_flag.get_value(), "Subsystem threw an unhandled exception."

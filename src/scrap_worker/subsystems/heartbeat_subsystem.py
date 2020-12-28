import logging
import time

from typing import List, Optional

from ..kafka import KafkaProducer
from ..models.event_models import (
    AcknowledgedWorkRequestEvent,
    CompletedWorkRequestEvent,
    HeartbeatEvent,
    LeaderChangedEvent,
)
from ..models.subsystem_config_models import HeartbeatSubsystemConfiguration
from ..state_machine.models import TimeoutFlag

_logger = logging.getLogger(__name__)


class HeartbeatSubsystem:
    def __init__(self, config: HeartbeatSubsystemConfiguration):
        self._config: HeartbeatSubsystemConfiguration = config
        self._count = 0
        self._leader_id: Optional[str] = None
        self._current_work_request_id: Optional[str] = None
        self._heartbeat_timeout = TimeoutFlag(
            self._config.heartbeat_delay_ms, "Time for heartbeat"
        )
        self._completed_work_request_ids: List[str] = []

    def _debug(self, msg: str):
        return
        _logger.debug(f"{msg}")

    def _broadcast_heartbeat(self, reason: str):
        self._debug(
            f"{str(self._config.worker_id)} Broadcasting heartbeat sequence {self._count} because {reason}"
        )
        self._count = (self._count + 1) % 1000000
        self._producer.produce(
            HeartbeatEvent(
                worker_id=str(self._config.worker_id),
                current_work_request_id=self._current_work_request_id,
                completed_work_request_ids=self._completed_work_request_ids,
                count=self._count,
                leader_id=self._leader_id,
            )
        )
        self._heartbeat_timeout.reset()

    def _should_exit(self):
        return self._config.should_exit_flag.get_value()

    def _configure_producer(self) -> KafkaProducer:
        return KafkaProducer(
            broker=self._config.get_kafka_producer_broker(),
            topic=self._config.heartbeat_topic,
        )

    def _get_leader_changed_event(self):
        latest: Optional[LeaderChangedEvent] = None
        while not self._config.leader_changed_event_queue.empty():
            latest = self._config.leader_changed_event_queue.get()
        if latest is None:
            return
        self._leader_id = latest.leader_id
        leader_id_str = self._leader_id if self._leader_id is not None else "None"
        self._debug(f"Consumed leader changed event. New leader is {leader_id_str} ")

    def _get_work_request_event(self):
        # TODO: This smells more than I expected. I'm not sure if this would be better suited for another
        # subsystem entirely.
        consuming = True
        while consuming:
            consuming = False
            if (
                self._current_work_request_id is None
                and not self._config.acknowledged_work_request_event_queue.empty()
            ):
                acknowledged_work_request_event: AcknowledgedWorkRequestEvent = (
                    self._config.acknowledged_work_request_event_queue.get()
                )
                if (
                    acknowledged_work_request_event.work_request_id
                    not in self._completed_work_request_ids
                ):
                    self._current_work_request_id = (
                        acknowledged_work_request_event.work_request_id
                    )
                    self._broadcast_heartbeat(
                        f"We are acknowledging the assignment of work request {self._current_work_request_id}."
                    )
                consuming = True
            if (
                self._current_work_request_id is not None
                and not self._config.completed_work_request_event_queue.empty()
            ):
                completed_work_request_event: CompletedWorkRequestEvent = (
                    self._config.completed_work_request_event_queue.get()
                )
                self._current_work_request_id = (
                    None
                    if self._current_work_request_id
                    == completed_work_request_event.work_request_id
                    else self._current_work_request_id
                )
                self._completed_work_request_ids.append(
                    completed_work_request_event.work_request_id
                )
                self._broadcast_heartbeat(
                    f"We are completing work request {completed_work_request_event.work_request_id}."
                )
                consuming = True

    def _unsafe_run(self):
        self._producer = self._configure_producer()
        while not self._should_exit():
            self._get_leader_changed_event()
            self._get_work_request_event()
            if self._heartbeat_timeout.get_value():
                self._broadcast_heartbeat(
                    "We need to let the leader know we're still alive."
                )

    def start(self):
        self._config.started_flag.set_value()
        try:
            self._unsafe_run()
        except Exception as ex:
            self._config.exception_flag.set_value(context={"ex": ex})
            _logger.exception("Unhandled exception in heartbeat subsystem.")
        finally:
            self._config.exited_flag.set_value()

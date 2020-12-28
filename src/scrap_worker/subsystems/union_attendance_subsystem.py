from __future__ import annotations

import json
import logging

from pydantic import BaseModel, parse_obj_as
from queue import Queue
from typing import Dict, Optional

from ..kafka import KafkaConsumer, KafkaMessageWrapper
from ..models.event_models import (
    AcknowledgedWorkRequestEvent,
    CompletedWorkRequestEvent,
    UnionMembershipChangedEvent,
    HeartbeatEvent,
)
from ..models.subsystem_config_models import UnionAttendanceSubsystemConfiguration
from ..state_machine.models import TimeoutFlag

_logger = logging.getLogger(__name__)


class UnionAttendanceSubsystem:
    """
    Used by the union leader to track union members by observing the heartbeat topic.
    """

    class AttendanceData(BaseModel):
        # Don't set a default here because we want the description
        # to include the worker id.
        timeout: TimeoutFlag
        last_event: HeartbeatEvent

        class Config:
            arbitrary_types_allowed: bool = True

    def __init__(self, config: UnionAttendanceSubsystemConfiguration):
        self._config: UnionAttendanceSubsystemConfiguration = config
        self._context_config = {
            "id": str(self._config.worker_id),
            "topic": self._config.heartbeat_topic,
            "broker": self._config.get_kafka_consumer_broker(),
        }
        self._members: Dict[str, UnionAttendanceSubsystem.AttendanceData] = {}

    def _debug(self, msg: str):
        return
        _logger.debug(f"{msg}")

    def _should_exit(self):
        return self._config.should_exit_flag.get_value()

    def _parse_heartbeat(self, raw_msg: KafkaMessageWrapper) -> HeartbeatEvent:
        raw_value = raw_msg.value()
        value_as_dict = json.loads(raw_value)
        msg = parse_obj_as(HeartbeatEvent, value_as_dict)
        return msg

    def _consume_heartbeat(self):
        raw_msg = self._consumer.consume()
        if raw_msg is None:
            return

        try:
            heartbeat = self._parse_heartbeat(raw_msg)
            self._update_attendance(heartbeat)
        except Exception:
            _logger.exception(
                f"Failed to consume heartbeat event. Skipping. Context: {str(self._context_config)}"
            )

        self._consumer.commit()

    def _expire_attendance(self):
        for worker_id in self._members.keys():
            member = self._members.get(worker_id)
            if not member.timeout.get_value():
                continue
            self._debug(member.timeout.get_description())
            expired = self._members.pop(worker_id)
            self._config.union_membership_changed_event_queue.put(
                UnionMembershipChangedEvent(
                    joining=False,
                    worker_id=worker_id,
                    last_heartbeat=expired.last_event,
                )
            )

    def _update_work_request_events(
        self, last_heartbeat: Optional[HeartbeatEvent], latest_heartbeat: HeartbeatEvent
    ):
        if last_heartbeat is None:
            last_heartbeat = HeartbeatEvent(
                worker_id=latest_heartbeat.worker_id,
                count=latest_heartbeat.count,
            )

        if (
            last_heartbeat.current_work_request_id is None
            and latest_heartbeat.current_work_request_id is not None
        ):
            self._debug(
                f"Letting leadership subsystem know worker {latest_heartbeat.worker_id} has acknowledged work request {latest_heartbeat.current_work_request_id}."
            )
            self._config.acknowledged_work_request_event_queue.put(
                AcknowledgedWorkRequestEvent(
                    worker_id=latest_heartbeat.worker_id,
                    work_request_id=latest_heartbeat.current_work_request_id,
                )
            )

        for completed_work_request_id in latest_heartbeat.completed_work_request_ids:
            if (
                completed_work_request_id
                not in last_heartbeat.completed_work_request_ids
            ):
                self._debug(
                    f"Letting leadership subsystem know worker {latest_heartbeat.worker_id} has completed work request {completed_work_request_id}."
                )
                self._config.completed_work_request_event_queue.put(
                    AcknowledgedWorkRequestEvent(
                        worker_id=latest_heartbeat.worker_id,
                        work_request_id=completed_work_request_id,
                    )
                )

    def _update_attendance(self, heartbeat: HeartbeatEvent):
        if heartbeat.worker_id == str(self._config.worker_id):
            # TODO: Might want to do something with our own heartbeat.
            return
        elif heartbeat.leader_id is None:
            # TODO: Might want to check if they used to think we were the leader.
            self._debug(
                f"Worker {heartbeat.worker_id} is reporting attendance but does not have a leader."
            )
            return
        elif heartbeat.leader_id != str(self._config.worker_id):
            _logger.warning(
                f"Worker {heartbeat.worker_id} is reporting attendance but recognizes {heartbeat.leader_id} as their leader."
            )
            return

        member_attendance = self._members.get(heartbeat.worker_id, None)
        if member_attendance is not None:
            last_heartbeat = member_attendance.last_event
            member_attendance.last_event = heartbeat
            self._update_work_request_events(last_heartbeat, heartbeat)
            member_attendance.timeout.reset()
            return

        self._debug(f"Worker {heartbeat.worker_id} is joining the union.")
        self._members[heartbeat.worker_id] = UnionAttendanceSubsystem.AttendanceData(
            timeout=TimeoutFlag(
                timeout_ms=10000,
                description=f"Worker {heartbeat.worker_id} timed out after 10 seconds.",
            ),
            last_event=heartbeat,
        )
        self._config.union_membership_changed_event_queue.put(
            UnionMembershipChangedEvent(
                joining=True,
                worker_id=heartbeat.worker_id,
                last_heartbeat=self._members[heartbeat.worker_id].last_event,
            )
        )
        self._update_work_request_events(None, heartbeat)

    def _take_attendance(self):
        self._consume_heartbeat()
        self._expire_attendance()

    def _configure_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            id=str(self._config.worker_id),
            topics=[self._config.heartbeat_topic],
            brokers=self._config.get_kafka_consumer_broker(),
        )

    def _unsafe_run(self):
        self._consumer = self._configure_consumer()
        with self._consumer:
            while not self._should_exit():
                self._take_attendance()

    def start(self):
        self._config.started_flag.set_value()
        try:
            self._unsafe_run()
        except Exception as ex:
            self._config.exception_flag.set_value(context={"ex": ex})
            _logger.exception("Unhandled exception in union attendance subsystem.")
        finally:
            self._config.exited_flag.set_value()

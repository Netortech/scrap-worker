from pydantic import BaseModel
from queue import Queue
from typing import List, Optional
from uuid import UUID

from .event_models import CandidacyEvent
from ..state_machine.models import StateFlag, Flag


class KafkaDependentSubsystemConfiguration(BaseModel):
    kafka_producer_broker: Optional[str]
    kafka_consumer_broker: Optional[str]

    def get_kafka_producer_broker(self):
        return self.kafka_producer_broker

    def get_kafka_consumer_broker(self):
        return (
            self.get_kafka_producer_broker()
            if self.kafka_consumer_broker is None
            else self.kafka_consumer_broker
        )


class GenericSubsystemConfiguration(BaseModel):
    worker_id: UUID

    exception_flag: StateFlag = StateFlag("GENERIC_SUBSYSTEM_EXCEPTION")
    started_flag: StateFlag = StateFlag("GENERIC_SUBSYSTEM_STARTED")
    exited_flag: StateFlag = StateFlag("GENERIC_SUBSYSTEM_EXITED")
    should_exit_flag: Flag

    class Config:
        arbitrary_types_allowed: bool = True


class HeartbeatSubsystemConfiguration(
    GenericSubsystemConfiguration, KafkaDependentSubsystemConfiguration, BaseModel
):
    heartbeat_topic: str
    heartbeat_delay_ms: int = 5000

    exception_flag: StateFlag = StateFlag("HEARTBEAT_SUBSYSTEM_EXCEPTION")
    started_flag: StateFlag = StateFlag("HEARTBEAT_SUBSYSTEM_STARTED")
    exited_flag: StateFlag = StateFlag("HEARTBEAT_SUBSYSTEM_EXITED")

    leader_changed_event_queue: Queue
    acknowledged_work_request_event_queue: Queue
    completed_work_request_event_queue: Queue


class UnionMembershipSubsystemConfiguration(
    GenericSubsystemConfiguration, KafkaDependentSubsystemConfiguration, BaseModel
):
    leader_id: Optional[str]
    union_members: List[str] = []
    leader_topic: str
    leadership_ttl_ms: int

    lost_leader_flag: StateFlag = StateFlag("UNION_MEMBERSHIP_LOST_LEADER")
    exception_flag: StateFlag = StateFlag("UNION_MEMBERSHIP_SUBSYSTEM_EXCEPTION")
    started_flag: StateFlag = StateFlag("UNION_MEMBERSHIP_SUBSYSTEM_STARTED")
    exited_flag: StateFlag = StateFlag("UNION_MEMBERSHIP_SUBSYSTEM_EXITED")

    leader_changed_event_queue: Queue
    acknowledged_work_request_event_queue: Queue
    completed_work_request_event_queue: Queue


class UnionLeadershipSubsystemConfiguration(
    GenericSubsystemConfiguration, KafkaDependentSubsystemConfiguration, BaseModel
):
    leader_topic: str
    max_announcement_delay_ms: int

    exception_flag: StateFlag = StateFlag("UNION_LEADERSHIP_SUBSYSTEM_EXCEPTION")
    started_flag: StateFlag = StateFlag("UNION_LEADERSHIP_SUBSYSTEM_STARTED")
    exited_flag: StateFlag = StateFlag("UNION_LEADERSHIP_SUBSYSTEM_EXITED")

    union_membership_changed_event_queue: Queue
    work_request_event_queue: Queue
    acknowledged_work_request_event_queue: Queue
    completed_work_request_event_queue: Queue


class QuellUprisingSubsystemConfiguration(
    GenericSubsystemConfiguration, KafkaDependentSubsystemConfiguration, BaseModel
):
    election_topic: str

    exception_flag: StateFlag = StateFlag("QUELL_UPRISING_SUBSYSTEM_EXCEPTION")
    started_flag: StateFlag = StateFlag("QUELL_UPRISING_SUBSYSTEM_STARTED")
    exited_flag: StateFlag = StateFlag("QUELL_UPRISING_SUBSYSTEM_EXITED")


class UnionAttendanceSubsystemConfiguration(
    GenericSubsystemConfiguration, KafkaDependentSubsystemConfiguration, BaseModel
):
    heartbeat_topic: str

    exception_flag: StateFlag = StateFlag("UNION_ATTENDANCE_SUBSYSTEM_EXCEPTION")
    started_flag: StateFlag = StateFlag("UNION_ATTENDANCE_SUBSYSTEM_STARTED")
    exited_flag: StateFlag = StateFlag("UNION_ATTENDANCE_SUBSYSTEM_EXITED")

    union_membership_changed_event_queue: Queue
    acknowledged_work_request_event_queue: Queue
    completed_work_request_event_queue: Queue


class ElectionSubsystemConfiguration(
    GenericSubsystemConfiguration, KafkaDependentSubsystemConfiguration, BaseModel
):
    ranking: int
    election_topic: str
    winning_candidacy: Optional[CandidacyEvent] = None

    countdown_start = 10
    # TODO: Consider only producing the next countdown once we've consumed it.
    countdown_delay_ms = 500
    # This should only make a difference in situations where the candidate
    # dropped in the middle of the election.
    campaign_ttl_ms = 5000

    elected_new_leader_flag: StateFlag = StateFlag(
        "ELECTION_SUBSYSTEM_ELECTED_NEW_LEADER"
    )
    elected_as_leader_flag: StateFlag = StateFlag(
        "ELECTION_SUBSYSTEM_ELECTED_AS_LEADER"
    )
    exception_flag: StateFlag = StateFlag("ELECTION_SUBSYSTEM_EXCEPTION")
    started_flag: StateFlag = StateFlag("ELECTION_SUBSYSTEM_STARTED")
    exited_flag: StateFlag = StateFlag("ELECTION_SUBSYSTEM_EXITED")


class WorkloadPreprocessorSubsystemConfiguration(
    GenericSubsystemConfiguration, KafkaDependentSubsystemConfiguration, BaseModel
):
    work_request_topic: str

    exception_flag: StateFlag = StateFlag("WORKLOAD_PREPROCESSOR_SUBSYSTEM_EXCEPTION")
    started_flag: StateFlag = StateFlag("WORKLOAD_PREPROCESSOR_SUBSYSTEM_STARTED")
    exited_flag: StateFlag = StateFlag("WORKLOAD_PREPROCESSOR_SUBSYSTEM_EXITED")

    work_request_event_queue: Queue

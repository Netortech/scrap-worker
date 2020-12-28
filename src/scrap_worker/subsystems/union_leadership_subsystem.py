import logging
import time

from typing import Dict, List, Optional

from ..kafka import KafkaProducer
from ..models.event_models import (
    AcknowledgedWorkRequestEvent,
    CompletedWorkRequestEvent,
    LeaderInstructionEvent,
    UnionMembershipChangedEvent,
    WorkRequestEvent,
)
from ..models.subsystem_config_models import UnionLeadershipSubsystemConfiguration
from ..state_machine.models import StateFlag, TimeoutFlag

_logger = logging.getLogger(__name__)


class WorkerAssignment:
    def __init__(
        self,
        worker_id: str,
        acknowledgement_timeout_ms: int = 5000,
        execution_timeout_ms: int = 30000,
    ):
        self._worker_id = worker_id
        self._acknowledgement_timeout_flag = TimeoutFlag(
            timeout_ms=acknowledgement_timeout_ms,
            description=f"Should be wrapped in a DescribedFlag",
        )
        self._execution_timeout_flag = TimeoutFlag(
            timeout_ms=execution_timeout_ms,
            description=f"Should be wrapped in a DescribedFlag",
        )
        self._assignment_acknowledged = StateFlag("WORK_ASSIGNMENT_ACKNOWLEDGED")
        self._completed_work_request = StateFlag("WORK_REQUEST_COMPLETED")
        self._current_work: Optional[WorkRequestEvent] = None
        self._last_assignment_ms: int = -1

    def _debug(self, msg: str):
        _logger.debug(f"[WorkerAssignment]: {msg}")

    # Note this is just for tracking assignments. Actually assigning the work involves publishing
    # to the leader topic which doesn't seem appropriate here.
    def set_assignment_pending_acknowledgement(self, work_request: WorkRequestEvent):
        self._acknowledgement_timeout_flag.reset()
        self._last_assignment_ms = TimeoutFlag.current_ts_ms()
        self._current_work = work_request

    def set_assignment_acknowledged_by_worker(self):
        self._execution_timeout_flag.reset()
        self._assignment_acknowledged.set_value()

    def set_work_request_completed(self):
        self._completed_work_request.set_value()

    def reset(self):
        self._assignment_acknowledged.reset()
        self._completed_work_request.reset()
        self._current_work = None
        # self._debug(
        #     f"Reset assignment for worker {str(self._worker_id)}. Ready for next assignment."
        # )

    def get_last_assignment_ms(self) -> int:
        return self._last_assignment_ms

    def get_worker_id(self) -> str:
        return self._worker_id

    def get_work_request_id(self) -> Optional[str]:
        return None if self._current_work is None else self._current_work.request_id

    def is_assigned_work(self) -> bool:
        return self._current_work is not None

    def poll(self):
        # TODO: Need to figure out how to handle failures a little better.
        # Failure modes:
        # 1. Worker fails to acknowledge within the timeout.
        #   a. Could be a zombie? (Heartbeat still works)
        #   b. Could be lagging. (Will eventually acknowledge and run the command)
        # 2. Worker fails to complete within the timeout.
        #   a. Could be a zombie? (Heartbeat still works)
        #   b. Could be stuck in an infinite loop.
        # Perhaps we should keep the worker in a suspended state until it responds to
        # some kind of "Hey are you ok?" message from the leader.
        if self._current_work is None:
            return

        if not self._assignment_acknowledged.get_value():
            if self._acknowledgement_timeout_flag.get_value():
                work_request = self._current_work
                self.reset()
                raise TimeoutError(
                    f"Timed out waiting for worker {self._worker_id} to acknowledge work request {work_request.request_id}."
                )

        if not self._completed_work_request.get_value():
            if self._execution_timeout_flag.get_value():
                work_request = self._current_work
                self.reset()
                raise TimeoutError(
                    f"Timed out waiting for worker {self._worker_id} to complete work request {work_request.request_id}."
                )

        if self._completed_work_request.get_value():
            self.reset()


class UnionLeadershipSubsystem:
    def __init__(self, config: UnionLeadershipSubsystemConfiguration):
        self._config = config
        self._announcement_timeout = TimeoutFlag(
            self._config.max_announcement_delay_ms,
            "Maximum announcement delay exceeded.",
        )
        self._producer: KafkaProducer = self._configure_producer()
        self._union_members: Dict[str, UnionMembershipChangedEvent] = {}
        self._worker_assignments: Dict[str, WorkerAssignment] = {}

    def _debug(self, msg: str):
        _logger.debug(f"[Leadership System]: {msg}")

    def _configure_producer(self) -> KafkaProducer:
        return KafkaProducer(
            broker=self._config.get_kafka_producer_broker(),
            topic=self._config.leader_topic,
        )

    def _broadcast_announcement(self, msg: LeaderInstructionEvent):
        self._announcement_timeout.reset()
        msg.union_members = list(self._union_members.keys())
        self._producer.produce(msg)

    def _broadcast_noop_announcement(self):
        # self._debug("Making NOOP announcement since we've been quiet for too long.")
        self._broadcast_announcement(
            LeaderInstructionEvent(
                worker_id=str(self._config.worker_id), op_code="NOOP"
            )
        )

    def _consume_membership_change(self):
        membership_change: UnionMembershipChangedEvent = (
            self._config.union_membership_changed_event_queue.get()
        )
        if membership_change.joining:
            self._union_members[membership_change.worker_id] = membership_change
            self._broadcast_announcement(
                LeaderInstructionEvent(
                    worker_id=str(self._config.worker_id),
                    op_code="ADD_MEMBER",
                    metadata={"member_id_added": membership_change.worker_id},
                )
            )
            if membership_change.worker_id not in self._worker_assignments:
                self._worker_assignments[
                    membership_change.worker_id
                ] = WorkerAssignment(
                    membership_change.worker_id,
                )
        else:
            self._union_members.pop(membership_change.worker_id)
            self._broadcast_announcement(
                LeaderInstructionEvent(
                    worker_id=str(self._config.worker_id),
                    op_code="REMOVE_MEMBER",
                    metadata={"member_id_removed": membership_change.worker_id},
                )
            )
            # Design Note: Don't pop the assignments here. Let them timeout and clean up
            # will occur in the _poll_worker_assignments method.

    def _consume_membership_changes(self):
        # Design Note: In theory this should be a pretty quiet
        # event queue with small bursts during start up, and
        # post elections. So processing everything in the queue
        # shouldn't have any serious performance implications.
        while not self._config.union_membership_changed_event_queue.empty():
            self._consume_membership_change()

    def _get_available_assignment_slots(self) -> List[WorkerAssignment]:
        return list(
            sorted(
                filter(
                    lambda x: not x.is_assigned_work(),
                    self._worker_assignments.values(),
                ),
                key=lambda x: x.get_last_assignment_ms(),
            )
        )

    def _consumed_work_request(self):
        """
        Consumes any available work requests assuming any assignment slots are available.
        """
        if self._config.work_request_event_queue.empty():
            return False

        self._debug(
            "Work request event queue has "
            + str(self._config.work_request_event_queue.qsize())
            + " items waiting."
        )

        available_assignment_slots = self._get_available_assignment_slots()

        if len(available_assignment_slots) == 0:
            _logger.warning(
                "Work requests are available but there are no free workers."
            )
            return False

        work_request = self._config.work_request_event_queue.get()
        next_available_assignment_slot = available_assignment_slots[0]
        self._debug(
            f"Worker {next_available_assignment_slot.get_worker_id()} available. Broadcasting assignment for work request {work_request.request_id}."
        )

        self._broadcast_announcement(
            LeaderInstructionEvent(
                worker_id=str(self._config.worker_id),
                op_code="ASSIGN_WORK_REQUEST",
                member_id=next_available_assignment_slot.get_worker_id(),
                work_request=work_request,
            )
        )
        next_available_assignment_slot.set_assignment_pending_acknowledgement(
            work_request=work_request
        )

        return True

    def _consume_work_requests(self):
        while self._consumed_work_request():
            pass

    def _poll_worker_assignments(self):
        for worker_assignment in self._worker_assignments.values():
            if worker_assignment.get_worker_id() not in self._union_members:
                self._worker_assignments.pop(worker_assignment.get_worker_id())
                continue

            if not worker_assignment.is_assigned_work():
                continue
            try:
                worker_assignment.poll()
                continue
            except Exception as ex:
                _logger.exception(
                    f"Poll failure on worker {worker_assignment.get_worker_id()} assignment."
                )

            # TODO: This work request wasn't completed. What do we do?
            # Probably should add to a local only retry queue. In order
            # to track how many retries were attempted we should wrap
            # the work request in another class. Might also help to includ
            # the exception above.
            worker_assignment.reset()

    def _consume_work_request_acknowledgements(self):
        # self._debug("Consuming work request acknowledgements...")

        while not self._config.acknowledged_work_request_event_queue.empty():
            acknowledged_work_request: AcknowledgedWorkRequestEvent = (
                self._config.acknowledged_work_request_event_queue.get()
            )
            if acknowledged_work_request.worker_id in self._worker_assignments:
                assignment: WorkerAssignment = self._worker_assignments[
                    acknowledged_work_request.worker_id
                ]
                if not assignment.is_assigned_work():
                    _logger.warning(
                        f"Worker {assignment.get_worker_id()} is acknowledging work request id {acknowledged_work_request.work_request_id} but we have not marked them as assigned work. It's likely they took too long to acknowledge the work."
                    )
                elif (
                    assignment.get_work_request_id()
                    == acknowledged_work_request.work_request_id
                ):
                    # self._debug(
                    #     f"Marking that worker {assignment.get_worker_id()} acknowledged our work request {assignment.get_work_request_id()} assignment in the allotted time."
                    # )
                    assignment.set_assignment_acknowledged_by_worker()
                else:
                    _logger.warning(
                        f"Worker {assignment.get_worker_id()} is acknowledging work request id {acknowledged_work_request.work_request_id} but we are waiting for acknowledgement on {assignment.get_work_request_id()}"
                    )
            else:
                _logger.warning(
                    f"Worker {assignment.get_worker_id()} is acknowledging work request id {acknowledged_work_request.work_request_id} but we don't have an assignment slot for them."
                )

        # self._debug("Done consuming work request acknowledgements.")

    def _consume_work_request_completions(self):
        if self._config.completed_work_request_event_queue.empty():
            # self._debug("There are no pending completion events.")
            return

        # self._debug("Consuming work request completions...")

        while not self._config.completed_work_request_event_queue.empty():
            completed_work_request: CompletedWorkRequestEvent = (
                self._config.completed_work_request_event_queue.get()
            )
            if completed_work_request.worker_id in self._worker_assignments:
                assignment: WorkerAssignment = self._worker_assignments[
                    completed_work_request.worker_id
                ]
                if not assignment.is_assigned_work():
                    _logger.warning(
                        f"Worker {assignment.get_worker_id()} has completed work request id {completed_work_request.work_request_id} but we have not marked them as assigned work. It's likely they took too long to acknowledge the work."
                    )
                elif (
                    assignment.get_work_request_id()
                    == completed_work_request.work_request_id
                ):
                    # self._debug(
                    #     f"Marking that worker {assignment.get_worker_id()} completed our work request {assignment.get_work_request_id()} assignment in the allotted time."
                    # )
                    assignment.set_work_request_completed()
                else:
                    _logger.warning(
                        f"Worker {assignment.get_worker_id()} is completed work request id {completed_work_request.work_request_id} but we are waiting for acknowledgement on {assignment.get_work_request_id()}"
                    )
            else:
                _logger.warning(
                    f"Worker {assignment.get_worker_id()} is completed work request id {completed_work_request.work_request_id} but we don't have an assignment slot for them."
                )

        # self._debug("Done consuming work request completions.")

    def _leadership_duty_poll(self):
        self._consume_membership_changes()
        self._consume_work_request_acknowledgements()
        self._consume_work_request_completions()

        self._poll_worker_assignments()

        self._consume_work_requests()

        if self._announcement_timeout.get_value():
            self._broadcast_noop_announcement()

    def _should_exit(self) -> bool:
        return self._config.should_exit_flag.get_value()

    def _unsafe_run(self):
        self._producer = self._configure_producer()

        while not self._should_exit():
            self._leadership_duty_poll()

    def start(self) -> None:
        self._config.started_flag.set_value()
        try:
            self._unsafe_run()
        except Exception as ex:
            self._config.exception_flag.set_value(context={"ex": ex})
            _logger.exception("Unhandled exception in union leadership subsystem.")
        finally:
            self._config.exited_flag.set_value()

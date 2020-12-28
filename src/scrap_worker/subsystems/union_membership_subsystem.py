import json
import logging

from pydantic import parse_obj_as
from typing import Any, Dict, List
from uuid import UUID

from ..kafka import KafkaConsumer, KafkaMessageWrapper
from ..models.event_models import (
    AcknowledgedWorkRequestEvent,
    CompletedWorkRequestEvent,
    LeaderInstructionEvent,
    LeaderChangedEvent,
    WorkRequestEvent,
)
from ..models.subsystem_config_models import UnionMembershipSubsystemConfiguration
from ..state_machine.models import TimeoutFlag

_logger = logging.getLogger(__name__)


class UnionMembershipSubsystem:
    """
    The second subsystem to start after the heartbeat subsystem.

    During normal operations this subsystem will be responsible for listening to leadership
    directions. The leader can step down by letting the union members know which will be
    captured by this subsystem. The leader is expected to send at least one event within
    a configurable timespan. If there are no leader operations to perform the leader can send
    a NOOP.

    During startup we first assume there is an existing membership. Because the heartbeats
    broadcasted by the heartbeat subsystem of this worker will be consumed by any existing leader
    we should see ourselves get added immediately under normal circumstances. If no leadership
    event is detected this subsystem will shut down and the subsystem manager will be
    responsible for starting the election subsystem.

    This subsystem will listen to leadership updates about the union as well as consume
    assignments.
    """

    def __init__(self, config: UnionMembershipSubsystemConfiguration):
        self._config = config
        self._context_config = {
            "id": str(self._config.worker_id),
            "topic": self._config.leader_topic,
            "broker": self._config.get_kafka_consumer_broker(),
        }
        self._lost_leader_timeout = TimeoutFlag(
            self._config.leadership_ttl_ms, "Timed out waiting for message from leader."
        )

    def _debug(self, msg: str):
        _logger.debug(f"{str(self._config.worker_id)}: {msg}")

    def _check_has_leader_gone_away(self):
        if self._config.leader_id is None:
            self._debug("Waiting for announcement from current leader...")

        if not self._lost_leader_timeout.get_value():
            return

        if self._config.leader_id is not None:
            self._debug("Leader lost.")
        else:
            self._debug(
                "No announcement was received. Possible causes:\n  1. This is a new union\n  2. The leader has died or lost connectivity to kafka."
            )
        self._config.leader_id = None
        self._config.lost_leader_flag.set_value()
        self._put_leader_changed_event()

    def _put_leader_changed_event(self):
        self._config.leader_changed_event_queue.put(
            LeaderChangedEvent(leader_id=self._config.leader_id)
        )

    # TODO: Get rid of this because it uses exec, eval.
    # This is only a first step. Raw code execution has a host of nearly
    # insolvable if not insolvable security concerns. Longer term we will only
    # allow passing pip package installation instructions, import paths and
    # function names with keyword arguments.
    def _run_unsafe_execution_work_request(self, msg: WorkRequestEvent):
        _logger.warning(
            f"Do not use {msg.op_code} in a production setting as it is extremely insecure. This is only provided for prototyping purposes."
        )

        def _run_in_block_to_avoid_name_collisions(
            code: str, method_name: str = "work"
        ):
            exec(code)
            return eval(f"{method_name}")()

        if msg.code is None:
            raise Exception(f"{msg.op_code} requires `code` value.")
        _run_in_block_to_avoid_name_collisions(msg.code)

    def _run_work_request(self, msg: WorkRequestEvent):
        operations: Dict[str, Any] = {
            "UNSAFE_EXECUTION": self._run_unsafe_execution_work_request,
        }

        self._debug(f"Leader has assigned us work request id {msg.request_id}.")

        if msg.op_code not in operations.keys():
            raise Exception(
                f"Leader instruction operation code {msg.op_code} not recognized. Allowed operations: {str(list(operations.keys()))}"
            )

        self._config.acknowledged_work_request_event_queue.put(
            AcknowledgedWorkRequestEvent(
                worker_id=str(self._config.worker_id),
                work_request_id=msg.request_id,
            )
        )
        try:
            start_time = TimeoutFlag.current_ts_ms()
            operations[msg.op_code](msg)
            execution_time = TimeoutFlag.current_ts_ms() - start_time
            self._debug(
                f"Work request id {msg.request_id} took {execution_time / 1000} seconds."
            )
        finally:
            self._config.completed_work_request_event_queue.put(
                CompletedWorkRequestEvent(
                    worker_id=str(self._config.worker_id),
                    work_request_id=msg.request_id,
                )
            )

    def _process_leader_instruction(self, msg: LeaderInstructionEvent):
        if self._config.leader_id is None:
            self._config.leader_id = msg.worker_id
            self._config.union_members = msg.union_members
            self._put_leader_changed_event()

        if str(self._config.leader_id) != str(UUID(msg.worker_id)):
            _logger.warning(
                f"Leader is {str(self._config.leader_id)} but {msg.worker_id} just broadcasted an instruction. Ignoring."
            )
            return

        # At this point we know it's an event from the leader so we can update the timestamp.
        self._lost_leader_timeout.reset()

        if msg.member_id != str(self._config.worker_id):
            return

        if msg.op_code == "ASSIGN_WORK_REQUEST":
            assert (
                msg.work_request is not None
            ), f"Work request attribute is required for leader instruction operation code {msg.op_code}."
            # TODO: Let the heartbeat subsystem know we're processing this request.
            self._run_work_request(msg.work_request)

    def _parse_leader_instruction(
        self, raw_msg: KafkaMessageWrapper
    ) -> LeaderInstructionEvent:
        raw_value = raw_msg.value()
        value_as_dict = json.loads(raw_value)
        msg: LeaderInstructionEvent = parse_obj_as(
            LeaderInstructionEvent, value_as_dict
        )
        return msg

    def _consume_leader_instruction(self):
        raw_msg = self._consumer.consume(500)
        if raw_msg is None:
            return

        try:
            instruction = self._parse_leader_instruction(raw_msg)
            self._process_leader_instruction(instruction)
        except Exception:
            _logger.exception(
                f"Failed to consume leader instruction event. Skipping. Context: {str(self._context_config)}"
            )

        self._consumer.commit()

    def _should_exit(self) -> bool:
        return (
            self._config.should_exit_flag.get_value()
            or self._config.lost_leader_flag.get_value()
        )

    def _configure_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            id=str(self._config.worker_id),
            topics=[self._config.leader_topic],
            brokers=self._config.get_kafka_consumer_broker(),
        )

    def _unsafe_run(self) -> None:
        self._consumer = self._configure_consumer()
        with self._consumer:
            while not self._should_exit():
                self._consume_leader_instruction()
                self._check_has_leader_gone_away()

    def start(self) -> None:
        self._config.started_flag.set_value()
        self._lost_leader_timeout.reset()
        self._put_leader_changed_event()
        try:
            self._unsafe_run()
        except Exception as ex:
            self._config.exception_flag.set_value(context={"ex": ex})
            _logger.exception("Unhandled exception in union membership subsystem.")
        finally:
            self._config.exited_flag.set_value()

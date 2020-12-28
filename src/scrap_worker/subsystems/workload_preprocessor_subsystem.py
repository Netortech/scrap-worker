import json
import logging

from pydantic import parse_obj_as

from ..kafka import KafkaConsumer, KafkaMessageWrapper
from ..models.event_models import WorkRequestEvent
from ..models.subsystem_config_models import WorkloadPreprocessorSubsystemConfiguration

_logger = logging.getLogger(__name__)


class WorkloadPreprocessorSubsystem:
    """
    Used by the union leader to consume any work requests and assign them to any available union members.
    """

    def __init__(self, config: WorkloadPreprocessorSubsystemConfiguration):
        self._config: WorkloadPreprocessorSubsystemConfiguration = config
        self._context_config = {
            "id": str(self._config.worker_id),
            "topic": self._config.work_request_topic,
            "broker": self._config.get_kafka_consumer_broker(),
        }

    def _debug(self, msg: str):
        return
        _logger.debug(f"[Workload Preprocessor]: {msg}")

    def _should_exit(self):
        return self._config.should_exit_flag.get_value()

    def _configure_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            id=str(self._config.worker_id),
            topics=[self._config.work_request_topic],
            brokers=self._config.get_kafka_consumer_broker(),
        )

    def _parse_work_request(self, raw_msg: KafkaMessageWrapper) -> WorkRequestEvent:
        raw_value = raw_msg.value()
        value_as_dict = json.loads(raw_value)
        msg: WorkRequestEvent = parse_obj_as(WorkRequestEvent, value_as_dict)
        return msg

    def _process_work_request(self, msg: WorkRequestEvent):
        self._debug("Forwarding work request on to leadership subsystem.")
        self._config.work_request_event_queue.put(msg)

    def _consume_work_request(self):
        # Design Note: There is an opportunity here to eat our own dog food.
        # We can issue the work request along with a request to save the work
        # to a data store. If neither of them pass we can retry a few times
        # and then drop to keep ourselves alive.
        # This would require batching up requests so we only commit once we've
        # cleared a batch. If there's a failure inside a batch we can skip the
        # commit and add any further requests up to the batch size.
        # I believe this will allow us to have multiple workload preprocessors
        # and leadership subsystems.
        raw_msg = self._consumer.consume(500)
        if raw_msg is None:
            return

        try:
            msg = self._parse_work_request(raw_msg)
            self._process_work_request(msg)
        except Exception:
            _logger.exception(
                f"Failed to consume work request event. Skipping. Context: {str(self._context_config)}"
            )

        self._consumer.commit()

    def _unsafe_run(self) -> None:
        self._consumer: KafkaConsumer = self._configure_consumer()
        with self._consumer:
            while not self._should_exit():
                self._consume_work_request()

    def start(self):
        self._config.started_flag.set_value()
        try:
            self._unsafe_run()
        except Exception as ex:
            self._config.exception_flag.set_value(context={"ex": ex})
            _logger.exception("Unhandled exception in workload preprocessor subsystem.")
        finally:
            self._config.exited_flag.set_value()

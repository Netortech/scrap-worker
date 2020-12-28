import json
import logging

from pydantic import parse_obj_as
from typing import Optional

from ..kafka import KafkaConsumer, KafkaMessageWrapper, KafkaProducer
from ..models.event_models import CandidacyEvent
from ..models.subsystem_config_models import QuellUprisingSubsystemConfiguration

_logger = logging.getLogger(__name__)


# TODO: Consider that if there are multiple uprisings from different workers we may want
# to resign as leader since it may be an indication we're not doing a good job.
# ... One may be inclined to think I'm intentionally drawing some parallel to reality
# here but I assure you it's uninentional.
class QuellUprisingSubsystem:
    """
    Used by a union leader to observe the election topic and make sure to quell any elections it observes
    by one upping any announced rankings.
    """

    def __init__(self, config: QuellUprisingSubsystemConfiguration):
        self._config: QuellUprisingSubsystemConfiguration = config
        self._candidacy: CandidacyEvent = CandidacyEvent(
            # We'll adjust this to be one higher than any uprising.
            ranking=1,
            worker_id=str(self._config.worker_id),
            acceptance_countdown=0,
        )
        self._context_config = {
            "id": str(self._config.worker_id),
            "topic": self._config.election_topic,
            "broker": self._config.get_kafka_consumer_broker(),
        }

    def _debug(self, msg: str):
        _logger.debug(f"{msg}")

    def _should_exit(self):
        return self._config.should_exit_flag.get_value()

    def _parse_candidacy(self, raw_msg: KafkaMessageWrapper) -> CandidacyEvent:
        raw_value = raw_msg.value()
        value_as_dict = json.loads(raw_value)
        msg = parse_obj_as(CandidacyEvent, value_as_dict)
        return msg

    def _consume_candidacy(self):
        raw_msg = self._consumer.consume()
        if raw_msg is None:
            return None

        try:
            candidacy = self._parse_candidacy(raw_msg)
            self._quell_uprising(candidacy)
        except Exception:
            _logger.exception(
                f"Failed to consume candidacy event. Skipping. Context: {str(self._context_config)}"
            )
            return

        self._consumer.commit()

    def _unsafe_broadcast_candidacy(self):
        self._producer.produce(self._candidacy)
        self._producer.flush()

    def _broadcast_candidacy(self, reason: str = None, retries: int = 0):
        while retries >= 0:
            try:
                self._unsafe_broadcast_candidacy()
                return
            # TODO: Be intelligent about the kinds of exceptions here and try to slow the
            # retries if it seems to be a connection issue.
            except Exception:
                _logger.exception(
                    f"Failed to broadcast candidacy. Context: {str(self._context_config)}"
                )
                retries = retries - 1
        raise Exception(
            "Failed to declare candidacy after maximum retries. See previous logs for possible causes."
        )

    def _quell_uprising(self, msg: CandidacyEvent):
        if msg.worker_id == self._candidacy.worker_id:
            return

        self._debug(f"Detected uprising from {msg.worker_id}. Quelling.")

        self._candidacy.ranking = msg.ranking + 1
        self._broadcast_candidacy()

    def _configure_producer(self) -> KafkaProducer:
        return KafkaProducer(
            broker=self._config.get_kafka_producer_broker(),
            topic=self._config.election_topic,
        )

    def _configure_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            id=str(self._config.worker_id),
            topics=[self._config.election_topic],
            brokers=self._config.get_kafka_consumer_broker(),
        )

    def _unsafe_run(self):
        self._producer = self._configure_producer()
        self._consumer = self._configure_consumer()
        with self._consumer:
            while not self._should_exit():
                self._consume_candidacy()

    def start(self):
        self._config.started_flag.set_value()
        try:
            self._unsafe_run()
        except Exception as ex:
            self._config.exception_flag.set_value(context={"ex": ex})
            _logger.exception("Unhandled exception in quell uprising subsystem.")
        finally:
            self._config.exited_flag.set_value()

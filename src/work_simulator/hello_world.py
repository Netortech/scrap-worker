import inspect
import logging
import time
import uuid


_logger = logging.getLogger(__name__)


def work():
    print("Hello world!")


def run_hello_world_execution(
    kafka_producer_klass, event_klass, config, delay_ms: int = 500
):
    producer = kafka_producer_klass(
        broker=config.kafka_producer_broker,
        topic=config.topic_prefix + config.work_request_topic,
    )
    while True:
        _logger.debug("Producing hello world work request...")
        producer.produce(
            event_klass(
                request_id=str(uuid.uuid4()),
                op_code="UNSAFE_EXECUTION",
                code=inspect.getsource(work),
            )
        )
        time.sleep(delay_ms / 1000)

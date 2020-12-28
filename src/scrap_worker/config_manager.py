import json
import logging
import os
import re
import uuid

from pydantic import BaseModel
from typing import Tuple, Optional

from .default_config import set_default_config

CONFIG_ENV_NAME = "X-SCRAP-WORKER-CONFIG"
_logger = logging.getLogger(__name__)


class Config(BaseModel):
    worker_id: uuid.UUID = uuid.uuid4()
    log_level: int = logging.WARNING

    kafka_producer_broker: str = "kafka:9092"
    kafka_consumer_broker: Optional[str] = None

    topic_prefix: str = "scrapworkerunion-1-"
    heartbeat_topic: str = "heartbeat"
    leader_topic: str = "leader"
    election_topic: str = "election"
    work_request_topic: str = "requests"
    ranking: int = 1


class Configurations:
    @staticmethod
    def test() -> Config:
        return Config(log_level=logging.INFO)

    @staticmethod
    def default() -> Config:
        config = Config()
        set_default_config(config)
        return config


def get_config() -> Config:
    config_name: str = os.environ.get(CONFIG_ENV_NAME, "")
    config_instance = getattr(Configurations, config_name, None)
    if not callable(config_instance):
        raise Exception(
            f"Configuration name set to {config_name} but no callable members of Configuration were found with that name."
        )
    return config_instance()

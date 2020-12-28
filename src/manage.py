import click
import logging
import sys
import os

_logger = logging.getLogger(__name__)


def setup_custom_config():
    """
    Checks for existence of default_config.py file. If not present it will
    create the file using the contents of default_config.template.

    default_config.py is included in .gitignore so you can make any
    changes you like to this file without accidentally including it
    in any check ins.
    """

    BASE_PATH = "scrap_worker"
    DEFAULT_CONFIG_FILE_NAME = os.path.join(BASE_PATH, "default_config.py")
    DEFAULT_CONFIG_TEMPLATE_NAME = os.path.join(BASE_PATH, "default_config.template")

    if os.path.exists(DEFAULT_CONFIG_FILE_NAME):
        return

    # The logger hasn't been configured at this point.
    print("Default configuration file not found. Creating.")

    try:
        with open(DEFAULT_CONFIG_TEMPLATE_NAME, "r") as template:
            with open(DEFAULT_CONFIG_FILE_NAME, "w+") as config:
                config.write(template.read())
    except Exception as ex:
        raise Exception(
            f"Failed to write configuration file {DEFAULT_CONFIG_FILE_NAME} from {DEFAULT_CONFIG_TEMPLATE_NAME}. "
            "If this is a permissions issue you can resolve this error by manually copying the template file yourself and then running again."
        ) from ex


def configure_logging():
    from scrap_worker.config_manager import get_config

    config = get_config()
    _rootLogger = logging.getLogger()
    _rootLogger.level = config.log_level
    _rootLogger.addHandler(logging.StreamHandler(sys.stdout))

    _logger.info("Logging configured for Scrap Worker application.")


@click.group()
def cli():
    pass


@cli.command()
def start():
    setup_custom_config()
    configure_logging()

    from scrap_worker.app_state_machine import (
        configure_default_manager,
    )

    configure_default_manager().start()


@cli.command()
def simulate_hello_world_work():
    setup_custom_config()
    configure_logging()

    from scrap_worker.config_manager import Config, get_config
    from scrap_worker.kafka import KafkaProducer
    from scrap_worker.models.event_models import WorkRequestEvent
    from work_simulator.hello_world import run_hello_world_execution

    config = get_config()

    run_hello_world_execution(
        kafka_producer_klass=KafkaProducer,
        event_klass=WorkRequestEvent,
        config=config,
    )


if __name__ == "__main__":
    cli()

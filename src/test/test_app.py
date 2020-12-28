from ..scrap_worker.app_state_machine import (
    configure_default_manager,
    ScrapWorkerSubsystemConfigurations,
)
from ..scrap_worker.state_machine.models import ReadOnlyFlag, StateFlag, TimeoutFlag


def test_app_configure_default_state_manager():
    global_exit_flag = StateFlag("GLOBAL_EXIT_FLAG")
    subsystem_configs = ScrapWorkerSubsystemConfigurations(
        global_exit_flag=ReadOnlyFlag(flag=global_exit_flag)
    )
    manager = configure_default_manager(
        global_exit_flag=global_exit_flag,
        subsystem_configs=subsystem_configs,
    )

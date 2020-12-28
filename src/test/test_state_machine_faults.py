from ..scrap_worker.state_machine.models import State, StateFlag
from ..scrap_worker.state_machine.state_manager import StateManager


def test_basic_timeout():
    """
    This test is designed to verify the behavior of the state timeout functionality.

    As an aside it also tests the following functionalities of the state manager
    non-blocking `start-thread` method:
    1. General execution
    2. Ability to detect a faulted state.
    3. Setting the value on any provided shutdown flags.
    Bugs any of these mechanisms can also cause the test to fail.
    """
    TIMEOUT = 0.1
    shutdown_flag = StateFlag("SHUTDOWN_FLAG")
    thread = StateManager(
        State("TIMEOUT_TEST", timeout_ms=TIMEOUT * 1000),
        faulted_state=State.get_default_faulted_state(),
        name="Timeout test",
        shutdown_flags=[shutdown_flag],
    ).start_thread()
    thread.join(timeout=TIMEOUT * 2)
    assert not thread.is_alive(), "State machine did not terminate after state timeout."
    assert (
        shutdown_flag.get_value()
    ), "State machine did not mark the shutdown flag before shutting down."

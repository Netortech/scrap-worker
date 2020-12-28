import logging
import threading

from typing import List, Optional

from .models import State, StateFlag

_logger = logging.getLogger(__name__)


class StateManager:
    def __init__(
        self,
        start_state: State,
        faulted_state: State = None,
        name: str = None,
        shutdown_flags: List[StateFlag] = [],
    ):
        assert start_state is not None, "Start state cannot be None."
        self._start_state: State = start_state
        self._current_state: State = start_state
        self._faulted_state: Optional[State] = faulted_state
        self._thread: Optional[threading.Thread] = None
        self._name: Optional[str] = name
        self._shutdown_flags = shutdown_flags if shutdown_flags is not None else []

    def _get_name_prefix(self):
        prefix = ""
        if self._name is not None:
            prefix = prefix + f"{self._name}: "
        return prefix

    def _debug(self, msg: str):
        _logger.debug(f"{self._get_name_prefix()}{msg}")

    def _unsafe_run(self):
        while True:
            self._current_state.execute()
            transition = self._current_state.get_activated_transition()
            if transition is None:
                continue
            self._debug(
                f"Transition {self._current_state.get_name()} -> {str(transition)}"
            )
            transition.get_state().reset()
            previous_state = self._current_state
            self._current_state = transition.get_state()
            if self._current_state == self._faulted_state:
                _logger.warning(
                    f"{self._get_name_prefix()}{str(transition)} Coming from state {previous_state.get_name()}. Exiting run loop. Look to previous log messages for possible causes."
                )
                return

    def start(self):
        """
        Blocking call to start the manager.
        """
        try:
            self._unsafe_run()
        except Exception:
            _logger.exception("Failure while managing state machine.")
        for shutdown_flag in self._shutdown_flags:
            shutdown_flag.set_value()

    def start_thread(self) -> threading.Thread:
        """
        Non-blocking call to start the manager.
        """
        assert self._thread is None, "Thread has already been created."

        self._thread = threading.Thread(target=self.start)
        self._thread.start()
        return self._thread

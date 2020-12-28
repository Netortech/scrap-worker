from __future__ import annotations

import logging
import math
import threading
import time


from typing import Any, Callable, Dict, List, Optional

_logger = logging.getLogger(__name__)


class Flag:
    def get_value(self) -> bool:
        raise NotImplementedError()

    def get_description(self) -> Optional[str]:
        return None


class ReadOnlyFlag(Flag):
    def __init__(self, flag: Flag):
        assert flag is not None, "Flag cannot be None."
        self._flag = flag

    def get_value(self) -> bool:
        return self._flag.get_value()

    def get_description(self) -> Optional[str]:
        return self._flag.get_description()

    def set_value(self, value: bool = True):
        raise Exception("Cannot set value on a ReadOnlyFlag.")


class AndFlag(Flag):
    def __init__(self, flags: List[Flag]):
        assert flags is not None, "Flags must be provided."
        assert len(flags) > 1, "At least two flags are required."
        self._flags = flags

    def get_value(self):
        for flag in self._flags:
            if not flag.get_value():
                return False
        return True


class OrFlag(Flag):
    def __init__(self, flags: List[Flag]):
        assert flags is not None, "Flags must be provided."
        assert len(flags) > 1, "At least two flags are required."
        self._flags = flags

    def get_value(self):
        for flag in self._flags:
            if flag.get_value():
                return True
        return False


class NotFlag(Flag):
    def __init__(self, flag: Flag):
        assert flag != None, "Flag parameter must not be None."
        self._flag = flag

    def get_value(self):
        return not self._flag.get_value()


class DescribedFlag(Flag):
    def __init__(self, flag: Flag, description: str):
        assert description != None, "Descriptions are required on described flags."
        self._flag = flag
        self._description: str = description

    def get_value(self) -> bool:
        return self._flag.get_value()

    def get_description(self) -> Optional[str]:
        return self._description


class StateFlag(Flag):
    """
    A stateful flag used to track state of a machine.
    """

    def __init__(
        self,
        name: str,
        initial_value: bool = False,
        single_trigger: bool = True,
        allow_reset: bool = True,
    ):
        """
        Constructor

        Required:
        name: Name of the flag.

        Optional:
        initial_value: Defaults to False
        single_trigger: In single trigger mode set_value can only be used to change the value of this flag once.
        allow_reset: Allows the flag to be reset to it's original state. Intended to be used in single trigger mode so a flag can be recycled.
        """
        assert name != None, "State flag requires a name."
        assert len(name) > 2, "State flag name must be more than 2 characters."
        self._name: str = name
        self._value: bool = initial_value
        self._initial_value: bool = initial_value
        self._context: Any = {}
        self._single_trigger: bool = single_trigger
        self._allow_reset: bool = allow_reset
        self._count: int = 0

    def _debug(self, msg: str):
        return
        _logger.debug(f"{self._name} flag: {msg}")

    def reset(self):
        assert self._allow_reset, "Reset mode not enabled."
        self._value = self._initial_value
        self._count = 0
        self._context = {}

    def set_value(self, value: bool = True, context: Any = {}) -> None:
        if self._value == value:
            return

        self._count = self._count + 1
        if self._single_trigger:
            assert (
                self._count == 1
            ), "Value can only be changed once when using single trigger mode."

        self._debug(f"{self._value} -> {value}")
        self._value = value
        self._context = context

    def get_value(self) -> bool:
        return self._value


class TimeoutFlag(Flag):
    @staticmethod
    def current_ts_ms() -> int:
        """
        Returns the current timestamp floored to whole milliseconds.
        """
        return math.floor(time.time() * 1000)

    def __init__(
        self, timeout_ms: int, description: str = None, readonly: bool = False
    ):
        self._timeout_ms = timeout_ms
        self._start_ts_ms = TimeoutFlag.current_ts_ms()
        self._readonly = readonly
        self._description = description

    def get_description(self) -> Optional[str]:
        return self._description

    def reset(self) -> None:
        assert (
            not self._readonly
        ), "Cannot reset timeout flag which has been initialized with a readonly flag."
        self._start_ts_ms = TimeoutFlag.current_ts_ms()

    def get_value(self) -> bool:
        return TimeoutFlag.current_ts_ms() - self._start_ts_ms > self._timeout_ms


class Transition:
    def __init__(
        self,
        state: State,
        criteria: List[Flag],
        ordinal: int = 0,
    ):
        assert state is not None, "State cannot be None."
        assert criteria is not None, "Criteria cannot be None."
        assert len(criteria) > 0, "Must have at least one criterion."
        self._state = state
        self._criteria = criteria
        self._ordinal = ordinal

    def get_ordinal(self) -> int:
        return self._ordinal

    def get_state(self) -> State:
        return self._state

    def add_criterion(self, criterion: Flag) -> Transition:
        assert criterion is not None, "Criteria cannot be None"
        assert criterion not in self._criteria, "Criterion already added to criteria."
        self._criteria.append(criterion)
        return self

    def get_any_criterion(self) -> Optional[Flag]:
        for criterion in self._criteria:
            if criterion.get_value():
                return criterion
        return None

    # Design Note: Stay away from implementing a `check_all_criteria` method
    # and instead use the `AndFlag` class to combine multiple flags. This
    # allows us to treat each defined criteria as atomic and leads to better
    # documentation opportunities.
    def check_any_criterion(self) -> bool:
        """
        Returns True if any criteria pass.
        """
        return self.get_any_criterion() is not None

    # TODO: Consider if this is the right method for this logic.
    # It might make more sense in a dedicated method.
    def __str__(self) -> str:
        result = self.get_state().get_name()
        passing_criterion = self.get_any_criterion()
        if passing_criterion is None:
            result = result + f" (No passing criteria.)"
        elif passing_criterion.get_description() is not None:
            result = result + f" (Criteria: {passing_criterion.get_description()})"
        return result


class State:
    __default_faulted_state: Optional[State] = None

    @staticmethod
    def get_default_faulted_state() -> State:
        """
        The recommended method for creating a faulted state. While you can define and track your own custom
        faulted state most machines can benefit from this default. Used by the constructor when the timeout_ms
        parameter is provided.

        Note: Since this faulted state is deemed irrecoverable as there are no configured transitions. Thus
        there should be no side effects to using the same faulted state across multiple state machines.
        """
        if State.__default_faulted_state is not None:
            return State.__default_faulted_state

        State.__default_faulted_state = State(name="FAULTED", readonly=True)
        return State.__default_faulted_state

    # TODO: Consider if the timeout_ms makes sense or if the faulted state
    # should require more manual work.
    def __init__(
        self,
        name,
        op: Optional[Callable] = None,
        timeout_ms: int = -1,
        readonly: bool = False,
    ):
        """
        A state for a state machine.

        Required:
        name: The name for the state.

        Optional:
        op: The operation for this state triggered by calling execute. This method is idempotent and subsequent calls will have no effect.
        timeout_ms: The timeout for this state. Uses the state provided by the `get_default_faulted_state` class method.
        readonly: Prevents changes to the state after construction when using public interfaces.
        """
        assert name is not None, "State name cannot be None."
        assert len(name) > 2, "State name must be three charactors or greater."
        assert (
            timeout_ms != 0
        ), "A timeout of zero will immediately time out and is not allowed. Use a negative number to disable."
        self._name = name
        self._transitions: List[Transition] = []
        self._readonly: bool = False
        self._default_transition: Optional[Transition] = None
        self._timeout_flag: Optional[TimeoutFlag] = None

        if timeout_ms > 0:
            self._timeout_flag = TimeoutFlag(timeout_ms, description="State timeout.")
            self.add_default_transition_criterion(
                criterion=self._timeout_flag, auto_define=True
            )

        self._op: Optional[Callable] = op
        self._executed: bool = False

        self._readonly = readonly

    # TODO: If we do want to add more support for tracking the faulted state we may
    # need to add a flag here which allows them to specify if this is a fault transition.
    def add_transition(
        self, transition: Transition, is_default: bool = False
    ) -> Transition:
        """
        Define any transitions away from this state. Can be used to add a custom faulted state though the recommended
        approach is to use the `timeout_ms` parameter and/or the `get_default_faulted_state` methods.
        """
        assert not self._readonly, "Cannot add transitions to a state set to readonly."
        assert transition is not None, "Transition cannot be None."
        assert transition not in self._transitions, "Transition already added."
        assert (
            not is_default or self._default_transition is None
        ), "A default transition has already been specified."

        self._transitions.append(transition)
        if is_default:
            self._default_transition = transition
        return transition

    def add_default_transition_criterion(
        self, criterion: Flag, auto_define: bool = True
    ) -> State:
        """
        Adds a criterion to the default transition. Will throw an error if none is specified unless the
        `auto_define` parameter is set.

        Required:
        criterion: The criterion for the default transition

        Optional:
        auto_define: If no default transition is set this parameter will create one using the`get_default_faulted_state` method.
        """
        assert (
            auto_define or self._default_transition is not None
        ), "No default transition specified and `auto_define` parameter set to False."
        if self._default_transition is None:
            self.add_transition(
                Transition(
                    state=State.get_default_faulted_state(),
                    criteria=[criterion],
                ),
                is_default=True,
            )
        else:
            self._default_transition.add_criterion(criterion)
        return self

    def reset(self):
        self._executed = False

    def execute(self):
        """
        Used to execute the method provided in the `op` argument of the constructor. Method is idempotent
        and thus any subsequent calls after the first will have no effect.

        If a positive `timeout_ms` value was supplied to the constructor this method will begin the timeout
        on the first call to this method.
        """
        if self._executed:
            return

        if self._timeout_flag is not None:
            self._timeout_flag.reset()

        if self._op is not None:
            self._op()

        self._executed = True

    # TODO: Maybe add a description and/or friendly name for states.
    def get_name(self) -> str:
        """
        Returns the name of this state.
        """
        return self._name

    # TODO: Consider adding a parameter to verify only one transition is true at a time.
    # May not make sense especially since we're relying on timeouts to determine
    # if we're in a faulted state. Could possibly only fail if more than one non-faulted
    # state evaluates to true but that would require we know what the faulted state
    # we haven't coded for.
    # Further thoughts: We've implemented a default transition which should be the only
    # other transition which might also be true so we may be able to implement the
    # check to make sure only one of the non-default transitions is true.
    def get_activated_transition(self) -> Optional[Transition]:
        """
        Returns the first transition, sorted by ordinal, which has criteria which evaluates to true.

        If set the default transition will always be evaluated first.
        """
        if (
            self._default_transition is not None
            and self._default_transition.check_any_criterion()
        ):
            return self._default_transition
        for transition in sorted(self._transitions, key=lambda x: x.get_ordinal()):
            if transition.check_any_criterion():
                return transition
        return None

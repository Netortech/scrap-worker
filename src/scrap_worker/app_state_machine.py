import threading

from queue import Queue

from .config_manager import get_config
from .state_machine.models import (
    AndFlag,
    DescribedFlag,
    NotFlag,
    OrFlag,
    ReadOnlyFlag,
    State,
    StateFlag,
    Flag,
    TimeoutFlag,
    Transition,
)
from .state_machine.state_manager import StateManager
from .models.subsystem_config_models import (
    ElectionSubsystemConfiguration,
    GenericSubsystemConfiguration,
    HeartbeatSubsystemConfiguration,
    QuellUprisingSubsystemConfiguration,
    UnionAttendanceSubsystemConfiguration,
    UnionLeadershipSubsystemConfiguration,
    UnionMembershipSubsystemConfiguration,
    WorkloadPreprocessorSubsystemConfiguration,
)
from .subsystems import (
    ElectionSubsystem,
    HeartbeatSubsystem,
    QuellUprisingSubsystem,
    UnionAttendanceSubsystem,
    UnionLeadershipSubsystem,
    UnionMembershipSubsystem,
    WorkloadPreprocessorSubsystem,
)

_app = get_config()
_global_exit_flag: StateFlag = StateFlag("GLOBAL_EXIT_FLAG")


class ScrapWorkerSubsystemConfigurations:
    def __init__(self, global_exit_flag: Flag):
        self.global_exit_flag = global_exit_flag

        self.leader_changed_event_queue_for_heartbeat_subsystem: Queue = Queue()
        self.acknowledged_work_request_event_queue_for_heartbeat_subsystem: Queue = (
            Queue()
        )
        self.completed_work_request_event_queue_for_heartbeat_subsystem: Queue = Queue()

        self.union_membership_changed_event_queue_for_union_leadership_subsystem: Queue = (
            Queue()
        )
        self.work_request_event_queue_for_union_leadership_subsystem: Queue = Queue()
        self.acknowledged_work_request_event_queue_for_union_leadership_subsystem: Queue = (
            Queue()
        )
        self.completed_work_request_event_queue_for_union_leadership_subsystem: Queue = (
            Queue()
        )

        self.heartbeat_subsystem_configuration: HeartbeatSubsystemConfiguration = HeartbeatSubsystemConfiguration(
            worker_id=_app.worker_id,
            kafka_producer_broker=_app.kafka_producer_broker,
            should_exit_flag=global_exit_flag,
            heartbeat_topic=_app.topic_prefix + _app.heartbeat_topic,
            heartbeat_delay_ms=5000,  # 5 seconds
            leader_changed_event_queue=self.leader_changed_event_queue_for_heartbeat_subsystem,
            acknowledged_work_request_event_queue=self.acknowledged_work_request_event_queue_for_heartbeat_subsystem,
            completed_work_request_event_queue=self.completed_work_request_event_queue_for_heartbeat_subsystem,
        )
        self.union_membership_subsystem_configuration: UnionMembershipSubsystemConfiguration = UnionMembershipSubsystemConfiguration(
            worker_id=_app.worker_id,
            kafka_producer_broker=_app.kafka_producer_broker,
            should_exit_flag=global_exit_flag,
            leader_topic=_app.topic_prefix + _app.leader_topic,
            leadership_ttl_ms=10000,  # 10 seconds
            leader_changed_event_queue=self.leader_changed_event_queue_for_heartbeat_subsystem,
            acknowledged_work_request_event_queue=self.acknowledged_work_request_event_queue_for_heartbeat_subsystem,
            completed_work_request_event_queue=self.completed_work_request_event_queue_for_heartbeat_subsystem,
        )
        self.election_subsystem_configuration: ElectionSubsystemConfiguration = (
            ElectionSubsystemConfiguration(
                worker_id=_app.worker_id,
                kafka_producer_broker=_app.kafka_producer_broker,
                should_exit_flag=global_exit_flag,
                ranking=_app.ranking * 1000,
                election_topic=_app.topic_prefix + _app.election_topic,
            )
        )
        self.union_leadership_subsystem_configuration: UnionLeadershipSubsystemConfiguration = UnionLeadershipSubsystemConfiguration(
            worker_id=_app.worker_id,
            kafka_producer_broker=_app.kafka_producer_broker,
            should_exit_flag=global_exit_flag,
            leader_topic=_app.topic_prefix + _app.leader_topic,
            max_announcement_delay_ms=1000,
            union_membership_changed_event_queue=self.union_membership_changed_event_queue_for_union_leadership_subsystem,
            work_request_event_queue=self.work_request_event_queue_for_union_leadership_subsystem,
            acknowledged_work_request_event_queue=self.acknowledged_work_request_event_queue_for_union_leadership_subsystem,
            completed_work_request_event_queue=self.completed_work_request_event_queue_for_union_leadership_subsystem,
        )
        self.quell_uprising_subsystem_configuration: QuellUprisingSubsystemConfiguration = QuellUprisingSubsystemConfiguration(
            worker_id=_app.worker_id,
            kafka_producer_broker=_app.kafka_producer_broker,
            should_exit_flag=global_exit_flag,
            election_topic=_app.topic_prefix + _app.election_topic,
        )
        self.union_attendance_subsystem_configuration: UnionAttendanceSubsystemConfiguration = UnionAttendanceSubsystemConfiguration(
            worker_id=_app.worker_id,
            kafka_producer_broker=_app.kafka_producer_broker,
            should_exit_flag=global_exit_flag,
            heartbeat_topic=_app.topic_prefix + _app.heartbeat_topic,
            union_membership_changed_event_queue=self.union_membership_changed_event_queue_for_union_leadership_subsystem,
            # TODO: Consider these might be better in a post processor subsystem.
            acknowledged_work_request_event_queue=self.acknowledged_work_request_event_queue_for_union_leadership_subsystem,
            completed_work_request_event_queue=self.completed_work_request_event_queue_for_union_leadership_subsystem,
        )
        self.workload_preprocessor_subsystem_configuration: WorkloadPreprocessorSubsystemConfiguration = WorkloadPreprocessorSubsystemConfiguration(
            worker_id=_app.worker_id,
            kafka_producer_broker=_app.kafka_producer_broker,
            should_exit_flag=global_exit_flag,
            work_request_topic=_app.topic_prefix + _app.work_request_topic,
            work_request_event_queue=self.work_request_event_queue_for_union_leadership_subsystem,
        )


def construct(
    subsystem_configs: ScrapWorkerSubsystemConfigurations,
):
    # Note: All op prefixed methods are going to be passed into
    # the state constructors so the state machine will trigger
    # them using the `execute` method. This method is idempotent
    # and thus we don't need to perform any duplicate run checks.
    def op_start_heartbeat():
        threading.Thread(
            target=HeartbeatSubsystem(
                subsystem_configs.heartbeat_subsystem_configuration
            ).start
        ).start()

    def op_start_union_membership():
        subsystem_configs.union_membership_subsystem_configuration.started_flag.reset()
        subsystem_configs.union_membership_subsystem_configuration.exited_flag.reset()
        subsystem_configs.union_membership_subsystem_configuration.lost_leader_flag.reset()

        winning_candidacy = (
            subsystem_configs.election_subsystem_configuration.winning_candidacy
        )
        if winning_candidacy is not None:
            subsystem_configs.union_membership_subsystem_configuration.leader_id = (
                winning_candidacy.worker_id
            )

        threading.Thread(
            target=UnionMembershipSubsystem(
                subsystem_configs.union_membership_subsystem_configuration
            ).start
        ).start()

    def op_start_election():
        subsystem_configs.election_subsystem_configuration.started_flag.reset()
        subsystem_configs.election_subsystem_configuration.exited_flag.reset()
        subsystem_configs.election_subsystem_configuration.elected_as_leader_flag.reset()
        subsystem_configs.election_subsystem_configuration.elected_new_leader_flag.reset()

        subsystem_configs.election_subsystem_configuration.winning_candidacy = None

        threading.Thread(
            target=ElectionSubsystem(
                subsystem_configs.election_subsystem_configuration
            ).start
        ).start()

    def op_lead_union():
        subsystem_configs.union_leadership_subsystem_configuration.started_flag.reset()
        subsystem_configs.union_leadership_subsystem_configuration.exited_flag.reset()

        threading.Thread(
            target=UnionLeadershipSubsystem(
                subsystem_configs.union_leadership_subsystem_configuration
            ).start
        ).start()

        subsystem_configs.quell_uprising_subsystem_configuration.started_flag.reset()
        subsystem_configs.quell_uprising_subsystem_configuration.exited_flag.reset()
        threading.Thread(
            target=QuellUprisingSubsystem(
                subsystem_configs.quell_uprising_subsystem_configuration
            ).start
        ).start()

        subsystem_configs.union_attendance_subsystem_configuration.started_flag.reset()
        subsystem_configs.union_attendance_subsystem_configuration.exited_flag.reset()
        threading.Thread(
            target=UnionAttendanceSubsystem(
                subsystem_configs.union_attendance_subsystem_configuration
            ).start
        ).start()

        subsystem_configs.workload_preprocessor_subsystem_configuration.started_flag.reset()
        subsystem_configs.workload_preprocessor_subsystem_configuration.exited_flag.reset()
        threading.Thread(
            target=WorkloadPreprocessorSubsystem(
                subsystem_configs.workload_preprocessor_subsystem_configuration
            ).start
        ).start()

    DEFAULT_TIMEOUT_MS = 30000
    # The initial state. We're waiting for the heartbeat subsystem to
    # start up. We will wait for 5 seconds for it to do so.
    start_state: State = State(
        "WAITING_FOR_HEARTBEAT_SUBSYSTEM",
        op=op_start_heartbeat,
        timeout_ms=DEFAULT_TIMEOUT_MS,
    )

    # Used in a few different states to trigger the default transition
    # if the heartbeat subsystem dies.
    # We could use the exception flag here but since there's never
    # a reason the heartbeat subsystem should exit unless the application
    # is shutting down we might as well capture any cases where
    # it's incorrectly exiting but there was no exception.
    heartbeat_stopped_criterion: Flag = DescribedFlag(
        flag=AndFlag(
            [
                subsystem_configs.heartbeat_subsystem_configuration.exited_flag,
                NotFlag(subsystem_configs.global_exit_flag),
            ]
        ),
        description="Heartbeat subsystem exited but the global exit flag was not set.",
    )

    # Defining the states and transitions all the way up to being a union member. We'll
    # re-use this in another transition where we have elected a new union leader.
    participating_in_union_state: State = (
        start_state.add_transition(
            # Note: In theory we could combine these two states but I wanted to see
            # how well this works in a series. When starting up the leadership related
            # subsystems we'll look into parallelization.
            Transition(
                state=State(
                    "WAITING_FOR_UNION_MEMBERSHIP_SUBSYSTEM",
                    op=op_start_union_membership,
                    timeout_ms=DEFAULT_TIMEOUT_MS,
                ),
                criteria=[
                    DescribedFlag(
                        flag=subsystem_configs.heartbeat_subsystem_configuration.started_flag,
                        description="Heartbeat subsystem has started.",
                    ),
                ],
            )
        )
        .get_state()
        .add_default_transition_criterion(heartbeat_stopped_criterion)
        .add_transition(
            Transition(
                state=State(
                    "PARTICIPATING_IN_UNION",
                ),
                criteria=[
                    DescribedFlag(
                        flag=subsystem_configs.union_membership_subsystem_configuration.started_flag,
                        description="Union membership subsystem has started.",
                    )
                ],
            )
        )
        .get_state()
        .add_default_transition_criterion(heartbeat_stopped_criterion)
        .add_default_transition_criterion(
            DescribedFlag(
                flag=subsystem_configs.union_membership_subsystem_configuration.exception_flag,
                description="The union membership subsystem threw an unhandled exception.",
            )
        )
    )

    # We can code all the way up to completing the election. At that point we need two transitions
    # one for becoming a union member and the other for becoming a union leader.
    electing_new_leader_state: State = (
        participating_in_union_state.add_transition(
            Transition(
                state=State(
                    name="WAITING_FOR_ELECTION_TO_START",
                    op=op_start_election,
                    timeout_ms=DEFAULT_TIMEOUT_MS,
                ),
                criteria=[
                    DescribedFlag(
                        flag=subsystem_configs.union_membership_subsystem_configuration.lost_leader_flag,
                        description="The union membership subsystem either lost communication with the union leader or could not find one.",
                    )
                ],
            )
        )
        .get_state()
        .add_transition(
            Transition(
                state=State(
                    name="ELECTING_NEW_LEADER",
                ),
                criteria=[
                    DescribedFlag(
                        flag=AndFlag(
                            [
                                subsystem_configs.union_membership_subsystem_configuration.exited_flag,
                                subsystem_configs.election_subsystem_configuration.started_flag,
                            ]
                        ),
                        description="The election subsystem has started and the union membership subsystem has exited.",
                    )
                ],
            )
        )
        .get_state()
        .add_default_transition_criterion(heartbeat_stopped_criterion)
        .add_default_transition_criterion(
            DescribedFlag(
                flag=subsystem_configs.election_subsystem_configuration.exception_flag,
                description="The election subsystem threw an unhandled exception.",
            )
        )
        .add_default_transition_criterion(
            DescribedFlag(
                flag=AndFlag(
                    [
                        subsystem_configs.election_subsystem_configuration.exited_flag,
                        NotFlag(
                            subsystem_configs.election_subsystem_configuration.elected_as_leader_flag
                        ),
                        NotFlag(
                            subsystem_configs.election_subsystem_configuration.elected_new_leader_flag
                        ),
                    ]
                ),
                description="The election subsystem ended but no leader was elected.",
            )
        )
    )

    # Complete the state loop for becoming a union member and going back
    # to electing a leader in the event that the leader is lost.
    electing_new_leader_state.add_transition(
        Transition(
            state=State(
                name="ELECTED_NEW_LEADER_AND_JOINING_UNION_AS_MEMBER",
                op=op_start_union_membership,
                timeout_ms=DEFAULT_TIMEOUT_MS,
            ),
            criteria=[
                DescribedFlag(
                    flag=subsystem_configs.election_subsystem_configuration.elected_new_leader_flag,
                    description="Election completed. Joining union as member.",
                )
            ],
        )
    ).get_state().add_transition(
        Transition(
            state=participating_in_union_state,
            criteria=[
                DescribedFlag(
                    flag=AndFlag(
                        [
                            subsystem_configs.election_subsystem_configuration.exited_flag,
                            subsystem_configs.union_membership_subsystem_configuration.started_flag,
                        ]
                    ),
                    description="Election subsystem has exited and the union membership subsystem has started.",
                )
            ],
        )
    )

    # Complete the state loop for becoming a union leader. In theory we should never lose this position so
    # we won't loop back. If something goes wrong the whole app should just shut down.
    electing_new_leader_state.add_transition(
        Transition(
            state=State(
                name="ELECTED_AS_LEADER_AND_LEADING_UNION",
                op=op_lead_union,
                timeout_ms=DEFAULT_TIMEOUT_MS,
            ),
            criteria=[
                DescribedFlag(
                    flag=subsystem_configs.election_subsystem_configuration.elected_as_leader_flag,
                    description="Election completed. Leading union.",
                )
            ],
        )
    ).get_state().add_transition(
        Transition(
            state=State(
                name="LEADING_UNION",
            ),
            criteria=[
                DescribedFlag(
                    flag=AndFlag(
                        [
                            subsystem_configs.election_subsystem_configuration.exited_flag,
                            # Design Note:
                            # A union leader needs to consume from multiple topics so rather than
                            # having the leadership subsystem consume from a topic and a Queue
                            # make each topic consumption process owned by a separate service and
                            # only have the union leader subsystem consume from the Queues.
                            # Also consider abstracting what the queues provide. For example
                            # the attendance subsystem can just queue up joins and removals.
                            subsystem_configs.union_leadership_subsystem_configuration.started_flag,
                            subsystem_configs.union_attendance_subsystem_configuration.started_flag,
                            subsystem_configs.quell_uprising_subsystem_configuration.started_flag,
                            subsystem_configs.workload_preprocessor_subsystem_configuration.started_flag,
                        ]
                    ),
                    description="Election subsystem has exited and the union leadership subsystems havd started.",
                )
            ],
        )
    ).get_state().add_default_transition_criterion(
        heartbeat_stopped_criterion
    ).add_default_transition_criterion(
        DescribedFlag(
            flag=OrFlag(
                flags=[
                    subsystem_configs.union_leadership_subsystem_configuration.exited_flag,
                    subsystem_configs.quell_uprising_subsystem_configuration.exited_flag,
                    subsystem_configs.workload_preprocessor_subsystem_configuration.exited_flag,
                ]
            ),
            description="One of the required subsystems for union leadership has stopped.",
        )
    )

    return start_state


def configure_default_manager(
    global_exit_flag: StateFlag = _global_exit_flag,
    subsystem_configs: ScrapWorkerSubsystemConfigurations = None,
) -> StateManager:
    set_subsystem_configs = (
        ScrapWorkerSubsystemConfigurations(
            global_exit_flag=ReadOnlyFlag(flag=global_exit_flag)
        )
        if subsystem_configs is None
        else subsystem_configs
    )
    return StateManager(
        construct(set_subsystem_configs),
        State.get_default_faulted_state(),
        name="ScrapWorker",
        shutdown_flags=[global_exit_flag],
    )

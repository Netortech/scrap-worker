import json
import logging
import math
import random
import time

from pydantic import parse_obj_as
from typing import Any, Dict, List, Optional

from ..kafka import KafkaConsumer, KafkaProducer
from ..models.event_models import CandidacyEvent
from ..models.subsystem_config_models import ElectionSubsystemConfiguration

_logger = logging.getLogger(__name__)


class ElectionSubsystem:
    def __init__(self, config: ElectionSubsystemConfiguration):
        self._config = config
        self._candidacy: CandidacyEvent = CandidacyEvent(
            worker_id=str(self._config.worker_id),
            ranking=self._config.ranking,
        )
        self._last_countdown_ts: int = 0
        self._context_config = {
            "id": str(self._config.worker_id),
            "topic": self._config.election_topic,
            "broker": self._config.get_kafka_consumer_broker(),
        }

        # This will eventually include us but only once we've seen our own broadcast.
        self._remaining_candidates: Dict[str, Dict[str, Any]] = {}
        self._have_entered_the_race: bool = False

    def _ms_since_last_countdown(self) -> int:
        return self._current_ts() - self._last_countdown_ts

    def _current_ts(self) -> int:
        return math.floor(time.time() * 1000)

    def _debug(self, msg: str, force=False):
        short_id = self._candidacy.worker_id.split("-")[0]
        is_running = self._candidacy.withdrawn_and_nominating_worker_id is None
        if not is_running and not force:
            return
        voted_text = (
            ""
            if self._candidacy.withdrawn_and_nominating_worker_id is None
            else "voted: "
            + self._candidacy.withdrawn_and_nominating_worker_id.split("-")[0]
        )
        running_status = (
            ("RUNNING" if is_running else "OBSERVING " + voted_text)
            + " ranking: "
            + str(self._candidacy.ranking)
        )
        _logger.debug(f"\n{short_id} ({running_status}): {msg}")

    def _unsafe_broadcast_candidacy(self):
        """
        One of the situations where we can end up in an election is we've lost the
        connection to Kafka. Put whatever logic is necessary to detect this situation
        outside of this method.
        """
        self._producer.produce(self._candidacy)
        self._producer.flush()

    def _broadcast_candidacy(self, reason: str, retries: int = 0):
        """
        Update the other candidates about the state of your candidacy.
        """
        self._debug(f"Broadcasting candidacy because {reason}")
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

    def _run_tiebreaker_if_necessary(self):
        # We don't need to do this if we're not campaigning
        if self._candidacy.withdrawn_and_nominating_worker_id is not None:
            # self._debug("Not looking for ties because we're not campaigning.")
            return
        elif (
            self._candidacy.withdrawn_and_nominating_worker_id is None
            and self._candidacy.worker_id not in self._remaining_candidates
        ):
            self._debug(
                "We're running but we haven't consumed our own campaign announcement."
            )
            return

        # Can't be a tie if there's only one candidate
        if len(self._remaining_candidates) == 1:
            return

        for candidate_id in self._remaining_candidates.keys():
            candidate_ranking = self._remaining_candidates[candidate_id][
                "candidacy"
            ].ranking
            all_ties = self._candidacy.ranking == candidate_ranking
            if not all_ties:
                self._debug(
                    f"No tie detected. Candidate {candidate_id} has a ranking of {candidate_ranking} and ours is {self._candidacy.ranking}."
                )
                return

        self._debug("Tie detected between all remaining candidates.")

        # We're all tied so let's randomly decrement somewhere between 0 and 10 of our rank.
        self._candidacy.ranking = self._candidacy.ranking - random.randint(0, 10)
        self._debug("Ranking downgraded.")

        # If we hit less than zero start over
        if self._candidacy.ranking < 0:
            self._candidacy.ranking = self._config.ranking
            self._debug(f"We have reset our ranking to {self._candidacy.ranking}.")
        self._broadcast_candidacy("a tie was detected")

    def _update_remaining_candidates(self, msg: Optional[CandidacyEvent]):
        # Expire any campaigns which have exceeded the campaign TTL
        for candidate_id in list(self._remaining_candidates.keys()):
            if (
                self._current_ts() - self._remaining_candidates[candidate_id]["ts"]
                > self._config.campaign_ttl_ms
            ):
                self._remaining_candidates.pop(candidate_id, None)
                if candidate_id == self._candidacy.worker_id:
                    self._debug("Our own campaign has expired.")
                    self._broadcast_candidacy("our campaign has expired")
                else:
                    self._debug(f"Campaign for candidate {candidate_id} has expired.")

        if msg is None:
            return

        if msg.withdrawn_and_nominating_worker_id is not None:
            if msg.worker_id == self._candidacy.worker_id:
                self._debug(
                    f"Consumed our vote for {str(msg.withdrawn_and_nominating_worker_id)}. Our campaign has ended."
                )
            elif self._candidacy.withdrawn_and_nominating_worker_id is None:
                self._debug(
                    f"Worker {msg.worker_id} has voted. Their campaign has ended."
                )
            self._remaining_candidates.pop(msg.worker_id, None)
        else:
            timestamp = self._current_ts()
            if msg.worker_id == self._candidacy.worker_id:
                self._debug(
                    f"Our most recent campaign announcement (ranking: {msg.ranking}) was consumed at {timestamp}."
                )
            # We don't really need to know when non-candidates are consuming candidate campaign announcements.
            elif self._candidacy.withdrawn_and_nominating_worker_id is None:
                self._debug(
                    f"Candidate {msg.worker_id} most recent campaign announcement (ranking: {msg.ranking}) was consumed at {timestamp}."
                )
            self._remaining_candidates[msg.worker_id] = {
                "candidacy": msg,
                "ts": timestamp,
            }

        campaigns = dict(
            map(
                lambda x: (
                    x[0],
                    {
                        "ms": self._current_ts() - x[1]["ts"],
                        "ranking": x[1]["candidacy"].ranking,
                    },
                ),
                self._remaining_candidates.items(),
            )
        )

        # This log gets pretty noisy from non-candidates so we'll suppress it.
        # The only reason non-candidates track the candidates is to verify the
        # winner was expected. But this isn't a hugely important behavior.
        if self._candidacy.withdrawn_and_nominating_worker_id is None:
            self._debug(f"Updated remaining candidates. Campaigns: {str(campaigns)}")

    def _get_candidacy_event(self) -> Optional[CandidacyEvent]:
        # TODO: Need to change this logic to move the consume and commit outside
        # of the try catch block. See the other subsystems for examples
        try:
            raw_msg = self._consumer.consume(wait_for_assignment=True)
            if raw_msg is None:
                return None
            self._consumer.commit()
        except Exception:
            _logger.exception(
                f"Failed to consume candidacy event. Context: {str(self._context_config)}"
            )
            return None

        try:
            raw_value = raw_msg.value()
            value_as_dict = json.loads(raw_value)
            msg = parse_obj_as(CandidacyEvent, value_as_dict)
        except Exception:
            _logger.exception(
                f"Failed to process event. Skipping {self._config.election_topic}:{str(raw_msg.partition())} offset {str(raw_msg.offset())}. Context: {str(self._context_config)} Message: {str(raw_value)}"
            )
            return None

        return msg

    def _did_we_win(self) -> bool:
        """
        We have won the elction if all three are true:
            1. We are still running
            2. We have started our acceptance countdown
            3. Our acceptance countdown hits zero
        """

        # First, check if we're still in the running.
        if self._candidacy.withdrawn_and_nominating_worker_id is not None:
            # We're not running.
            return False

        # Second, have we started the acceptance countdown. This can be aborted elsewhere
        # by setting this value to None.
        if self._candidacy.acceptance_countdown is not None:
            # We've started our countdown already. Let's see if we're ready to broadcast the
            # next countdown.
            ms_since_last_countdown = self._ms_since_last_countdown()
            if ms_since_last_countdown > self._config.countdown_delay_ms:
                self._candidacy.acceptance_countdown = (
                    self._candidacy.acceptance_countdown - 1
                )
                self._last_countdown_ts = self._current_ts()
                self._broadcast_candidacy(
                    f"no one has contested our countdown in {ms_since_last_countdown} ms. Reducing to {self._candidacy.acceptance_countdown}."
                )

        # Third, did our countdown reach zero
        return self._candidacy.acceptance_countdown == 0

    def _should_start_acceptance_countdown(self) -> bool:
        # Make sure
        #  1. We haven't already started the countdown
        #  2. We are in the running
        #  3. We haven't voted (Note: We remain in the running until we see our own vote in the topic)
        if (
            self._candidacy.acceptance_countdown is not None
            or self._candidacy.worker_id not in self._remaining_candidates
            or self._candidacy.withdrawn_and_nominating_worker_id is not None
        ):
            return False

        # There's only one remaining candidate and we already have confirmed
        # one of the remaining candidate is us.
        # Note: Any expired campaigns are removed by _update_remaining_candidates
        return len(self._remaining_candidates) == 1

    def _are_campaigns_running(self) -> bool:  # noqa: C901
        """
        As a participant and a candidate you have two choices. You can stay in the race or you
        can vote by conceding to an opponent. Any votes you received will be transferred to the
        candidate you concede to.

        When you see a candidate cast their vote for you broadcast you have received the vote
        by adding it to your candidacy tally.

        Note: Do not count your concession until you see your opponent accept it by adding you
        to their vote tally.

        The election is over when any candidate issues an acceptance countdown marked
        as zero. The candidate should give ample warning before doing this by issuing
        acceptance countdowns greater than zero and should stop the moment it receives an
        event from another candidate.
        """
        if self._should_start_acceptance_countdown():
            # We believe everyone else has voted or dropped out.
            self._debug(
                f"We believe we have won. Starting acceptance countdown at {self._config.countdown_start}. Remaining candidates: {str(self._remaining_candidates)}"
            )
            self._candidacy.acceptance_countdown = self._config.countdown_start
            self._last_countdown_ts = self._current_ts()
            self._broadcast_candidacy("we believe we have won")

        if self._did_we_win():
            self._debug(f"We have accepted the leadership position.")
            self._config.winning_candidacy = self._candidacy
            self._config.elected_as_leader_flag.set_value()
            return False

        msg = self._get_candidacy_event()
        self._update_remaining_candidates(msg)
        self._run_tiebreaker_if_necessary()

        if not self._have_entered_the_race:
            self._broadcast_candidacy("we are entering the race")
            self._have_entered_the_race = True

        if msg is None:
            return True

        # Someone accepted the position. At the end of the day we just need a leader
        # so there's no point in contesting. We can issue a warning for various situations
        # just to make sure there's no bugs in the process.
        if msg.acceptance_countdown == 0:
            if msg.worker_id == self._candidacy.worker_id:
                _logger.exception(
                    f"We shouldn't be consuming our own zero count acceptance. Review the logic around {self._did_we_win.__name__}.",
                )
            elif msg.worker_id not in self._remaining_candidates:
                _logger.warning(
                    f"Worker ID {str(msg.worker_id)} declared themselves leader but were not considered a candidate. Remaining candidates: {str(self._remaining_candidates)}",
                )
            elif len(self._remaining_candidates) != 1:
                _logger.warning(
                    f"Worker ID {str(msg.worker_id)} declared themselves leader before all candidates had voted. Remaining candidates: {str(self._remaining_candidates)}",
                )
            self._config.winning_candidacy = msg
            self._config.elected_new_leader_flag.set_value()
            return False

        # Someone is voting
        if msg.withdrawn_and_nominating_worker_id is not None:
            did_we_vote_for_them = (
                self._candidacy.withdrawn_and_nominating_worker_id == msg.worker_id
            )
            did_they_vote_for_us = (
                msg.withdrawn_and_nominating_worker_id == self._candidacy.worker_id
            )

            # Depending on how votes are consumed sometimes two non-candidates have voted for each
            # other. In this case at least one of them should re-establish their candidacy. It's
            # okay if more than one does it will put us back into a ranking race.
            if (
                did_we_vote_for_them
                and did_they_vote_for_us
                and msg.ranking <= self._candidacy.ranking
            ):
                self._candidacy.withdrawn_and_nominating_worker_id = None
                self._broadcast_candidacy(
                    f"member {msg.worker_id} voted for us and we voted for them. Since their ranking of {msg.ranking} is less than or equal to ours {self._candidacy.ranking} we are resubmitting our candidacy."
                )
                return True

            if (
                did_we_vote_for_them
                and did_they_vote_for_us
                and msg.ranking > self._candidacy.ranking
            ):
                self._broadcast_candidacy(
                    f"member {msg.worker_id} voted for us and we voted for them. Since their ranking of {msg.ranking} greater ours {self._candidacy.ranking} we are resubmitting out vote for them."
                )
                return True

            if did_we_vote_for_them:
                self._candidacy.withdrawn_and_nominating_worker_id = (
                    msg.withdrawn_and_nominating_worker_id
                )
                self._broadcast_candidacy(
                    f"member {msg.worker_id} voted for Candidate {msg.withdrawn_and_nominating_worker_id} and we will follow."
                )
                return True

            # If they voted for us then add them to our tally.
            # Note: We don't need to update votes cast for this candidate.
            # It's the responsibility of the voters to update their votes.
            if msg.withdrawn_and_nominating_worker_id == self._candidacy.worker_id:
                if msg.worker_id not in self._candidacy.votes_received:
                    self._candidacy.votes_received.append(msg.worker_id)
                    self._broadcast_candidacy(
                        f"member {msg.worker_id} voted for us. Woohoo!"
                    )
            # If they voted for someone else let's make sure to remove them
            # from our votes received list.
            elif msg.withdrawn_and_nominating_worker_id != self._candidacy.worker_id:
                if msg.worker_id in self._candidacy.votes_received:
                    self._candidacy.votes_received.remove(msg.worker_id)
                    self._broadcast_candidacy(
                        f"member {msg.worker_id} voted for us but then voted for someone else. :("
                    )
            return True

        # If we're no longer running we can skip any further logic.
        if self._candidacy.withdrawn_and_nominating_worker_id is not None:
            return True

        # If this event is from us there's nothing else for us to do.
        if msg.worker_id == self._candidacy.worker_id:
            return True

        # Any non-vote messages not from us are cause to abort an acceptance countdown.
        # Check if we've started one and broadcast us aborting it if so.
        if self._candidacy.acceptance_countdown is not None:
            # Terminate the broadcast, add them as a remaining candidate but reassert our candidacy.
            self._candidacy.acceptance_countdown = None
            self._broadcast_candidacy(
                f"candidate {msg.worker_id} reaffirmed their campaign. Aborting our acceptance countdown."
            )

        # A candidate with a lower ranking has entered the race or updated their campaign. Let's broadcast our campaign again.
        # Note: If we don't do this candidates who reduce their ranks get caught in spin locks.
        if msg.ranking < self._candidacy.ranking:
            self._broadcast_candidacy(
                f"a candidate with a lower ranking than ours has entered the race."
            )
        # A candidate with a higher ranking has entered the race or updated their campaign. Give them our vote.
        elif msg.ranking > self._candidacy.ranking:
            self._candidacy.withdrawn_and_nominating_worker_id = msg.worker_id
            self._broadcast_candidacy(
                f"giving candidate {msg.worker_id} our vote because their ranking {msg.ranking} is greater than ours {self._candidacy.ranking}."
            )
        return True

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

    def _should_exit(self) -> bool:
        return self._config.should_exit_flag.get_value()

    def _unsafe_run(self):
        # Design Note: We need to consume our own campaign announcement
        # so we need to broadcast it
        self._producer = self._configure_producer()
        self._consumer = self._configure_consumer()
        with self._consumer:
            while not self._should_exit() and self._are_campaigns_running():
                pass

    def start(self):
        self._config.started_flag.set_value()
        try:
            self._unsafe_run()
        except Exception as ex:
            self._config.exception_flag.set_value(context={"ex": ex})
            _logger.exception("Unhandled exception in election subsystem.")
        finally:
            self._config.exited_flag.set_value()

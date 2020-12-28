from pydantic import BaseModel
from typing import Dict, List, Optional


class CandidacyEvent(BaseModel):
    worker_id: str
    ranking: int
    votes_received: List[str] = []
    acceptance_countdown: Optional[int] = None
    withdrawn_and_nominating_worker_id: Optional[str] = None


class LeaderChangedEvent(BaseModel):
    leader_id: Optional[str] = None


class HeartbeatEvent(BaseModel):
    worker_id: str
    count: int
    leader_id: Optional[str]
    current_work_request_id: Optional[str]
    completed_work_request_ids: List[str] = []


class UnionMembershipChangedEvent(BaseModel):
    joining: bool
    worker_id: str
    last_heartbeat: HeartbeatEvent


class WorkRequestEvent(BaseModel):
    request_id: Optional[str] = None
    op_code: str
    code: Optional[str]
    parameters: Optional[Dict[str, str]]

    # TODO: Potential future designs for validationg request
    # permissions.
    requester_id: Optional[str]
    signature: Optional[str]


class AcknowledgedWorkRequestEvent(BaseModel):
    worker_id: str
    work_request_id: str


class CompletedWorkRequestEvent(BaseModel):
    worker_id: str
    work_request_id: str


class LeaderInstructionEvent(BaseModel):
    worker_id: str
    op_code: str

    metadata: dict = {}
    union_members: List[str] = []

    member_id: Optional[str] = None
    work_request: Optional[WorkRequestEvent] = None

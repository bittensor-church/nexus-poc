import datetime
from typing import Any, NewType

from pydantic import BaseModel

from ..dsl.nodes import SourceId

ContextId = NewType("ContextId", str)
StepIdx = NewType("StepIdx", int)


class InvalidContextIdException(Exception):
    pass


class InvalidLogEntryIdException(Exception):
    pass


class MessageSent(BaseModel):
    """
    Log entry representing a message sent event.
    """

    source: SourceId
    payload_delta: bytes


class UserDataChange(BaseModel):
    """
    Log entry representing a message sent event.
    """

    key: str
    value_delta: bytes


class UserNote(BaseModel):
    """
    Log entry representing a custom user note added to the context log.
    """

    note: str


class ParentContextSnapshot(BaseModel):
    """
    Snapshot of a parent context captured when a child context is created.
    """

    ctx_id: ContextId
    payload: Any
    user_data: dict[str, Any]


class ContextDataInitialized(BaseModel):
    """
    Log entry representing child context initialization from parent context state.
    """

    payload: Any
    user_data: dict[str, Any]
    parent_contexts: tuple[ParentContextSnapshot, ...]


class ChildContextCreated(BaseModel):
    """
    Log entry representing the creation of a child context by the current context.
    """

    child_ctx: ContextId


class ContextCreated(BaseModel):
    """
    Log entry representing the creation of a context, possibly by multiple parent contexts.
    """

    parents: tuple[ContextId, ...]


class ContextCompleted(BaseModel):
    """
    Log entry representing completion of context processing.
    """

    pass


type LogEntryData = (
    MessageSent
    | UserDataChange
    | UserNote
    | ContextDataInitialized
    | ChildContextCreated
    | ContextCreated
    | ContextCompleted
)


LogEntryId = NewType("LogEntryId", int)


class LogEntry(BaseModel):
    ctx: ContextId
    step_idx: StepIdx
    creation_time: datetime.datetime
    data: LogEntryData

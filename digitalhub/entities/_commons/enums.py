from __future__ import annotations

from enum import Enum


class EntityTypes(Enum):
    """
    Entity types.
    """

    PROJECT = "project"
    ARTIFACT = "artifact"
    DATAITEM = "dataitem"
    MODEL = "model"
    SECRET = "secret"
    FUNCTION = "function"
    WORKFLOW = "workflow"
    TASK = "task"
    RUN = "run"


class Relationship(Enum):
    """
    Relationship enumeration.
    """

    PRODUCEDBY = "produced_by"
    CONSUMES = "consumes"
    RUN_OF = "run_of"


class State(Enum):
    """
    State enumeration.
    """

    BUILT = "BUILT"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    CREATED = "CREATED"
    CREATING = "CREATING"
    DELETED = "DELETED"
    DELETING = "DELETING"
    ERROR = "ERROR"
    FSM_ERROR = "FSM_ERROR"
    IDLE = "IDLE"
    NONE = "NONE"
    ONLINE = "ONLINE"
    PENDING = "PENDING"
    READY = "READY"
    RESUME = "RESUME"
    RUN_ERROR = "RUN_ERROR"
    RUNNING = "RUNNING"
    STOP = "STOP"
    STOPPED = "STOPPED"
    SUCCESS = "SUCCESS"
    UNKNOWN = "UNKNOWN"


class ApiCategories(Enum):
    """
    Entity categories.
    """

    BASE = "base"
    CONTEXT = "context"


class BackendOperations(Enum):
    """
    Backend operations.
    """

    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LIST = "list"
    LIST_FIRST = "list_first"
    STOP = "stop"
    RESUME = "resume"
    DATA = "data"
    FILES = "files"
    LOGS = "logs"
    SEARCH = "search"
    SHARE = "share"

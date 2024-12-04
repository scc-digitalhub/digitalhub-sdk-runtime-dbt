from __future__ import annotations

from enum import Enum


class EntityKinds(Enum):
    """
    Entity kinds.
    """

    FUNCTION_DBT = "dbt"
    TASK_DBT_TRANSFORM = "dbt+transform"
    RUN_DBT = "dbt+run"


class TaskActions(Enum):
    """
    Task actions.
    """

    TRANSFORM = "transform"

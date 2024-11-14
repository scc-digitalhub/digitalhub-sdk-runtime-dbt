from __future__ import annotations

from pydantic import Field

from digitalhub.entities.task._base.spec import TaskSpecFunction, TaskValidatorFunction


class TaskSpecContainerJob(TaskSpecFunction):
    """
    TaskSpecContainerJob specifications.
    """

    def __init__(
        self,
        function: str,
        node_selector: list[dict] | None = None,
        volumes: list[dict] | None = None,
        resources: dict | None = None,
        affinity: dict | None = None,
        tolerations: list[dict] | None = None,
        envs: list[dict] | None = None,
        secrets: list[str] | None = None,
        profile: str | None = None,
        backoff_limit: int | None = None,
        schedule: str | None = None,
        fsGroup: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            function,
            node_selector,
            volumes,
            resources,
            affinity,
            tolerations,
            envs,
            secrets,
            profile,
            **kwargs,
        )
        self.backoff_limit = backoff_limit
        self.schedule = schedule
        self.fsGroup = fsGroup


class TaskValidatorContainerJob(TaskValidatorFunction):
    """
    TaskValidatorContainerJob validator.
    """

    backoff_limit: int = Field(default=None, ge=0)
    """Backoff limit."""

    schedule: str = None
    """Schedule."""

    fsGroup: int = Field(default=None, ge=1)
    """FSGroup."""

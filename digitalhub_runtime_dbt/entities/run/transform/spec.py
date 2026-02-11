# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from digitalhub.entities.run._base.spec import RunSpec, RunValidator


class RunSpecDbtRun(RunSpec):
    """RunSpecDbtRun specifications."""

    def __init__(
        self,
        task: str,
        function: str | None = None,
        workflow: str | None = None,
        volumes: list[dict] | None = None,
        resources: dict | None = None,
        envs: list[dict] | None = None,
        secrets: list[str] | None = None,
        profile: str | None = None,
        source: dict | None = None,
        inputs: dict | None = None,
        outputs: dict | None = None,
        parameters: dict | None = None,
        local_execution: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            task,
            function,
            workflow,
            volumes,
            resources,
            envs,
            secrets,
            profile,
            **kwargs,
        )
        self.source = source
        self.inputs = inputs
        self.outputs = outputs
        self.parameters = parameters
        self.local_execution = local_execution


class RunValidatorDbtRun(RunValidator):
    """RunValidatorDbtRun validator."""

    # Function parameters
    source: dict | None = None

    # Run parameters
    inputs: dict | None = None
    """Run inputs."""

    outputs: dict | None = None
    """Run outputs."""

    parameters: dict | None = None
    """Run parameters."""

    local_execution: bool = False
    """Whether to execute the run locally instead of in the cluster."""

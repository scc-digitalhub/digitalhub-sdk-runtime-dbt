# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from digitalhub.entities.run._base.spec import RunSpec


class RunSpecDbtRun(RunSpec):
    """Specification for a dbt transform run."""

    source: dict | None = None

    inputs: dict | None = None
    """Run inputs."""

    outputs: dict | None = None
    """Run outputs."""

    parameters: dict | None = None
    """Run parameters."""

    local_execution: bool = False
    """Whether to execute the run locally instead of in the cluster."""

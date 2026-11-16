# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from digitalhub.entities.function._base.spec import FunctionSpec

from digitalhub_runtime_dbt.entities.function.dbt.models import DbtSourceCode


class FunctionSpecDbt(FunctionSpec):
    """Specification for a dbt function."""

    source: DbtSourceCode
    """Source code."""

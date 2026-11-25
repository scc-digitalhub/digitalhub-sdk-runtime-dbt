# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from digitalhub.plugins import FunctionBuilder, SourceBuilderMixin, Status

from digitalhub_runtime_dbt.entities._base.runtime_entity.builder import RuntimeEntityBuilderDbt
from digitalhub_runtime_dbt.entities._commons.enums import EntityKinds
from digitalhub_runtime_dbt.entities.function.dbt.entity import FunctionDbt
from digitalhub_runtime_dbt.entities.function.dbt.spec import FunctionSpecDbt


class FunctionDbtBuilder(SourceBuilderMixin, FunctionBuilder, RuntimeEntityBuilderDbt):
    """
    FunctionDbt builder.
    """

    ENTITY_CLASS = FunctionDbt
    ENTITY_SPEC_CLASS = FunctionSpecDbt
    ENTITY_STATUS_CLASS = Status
    ENTITY_KIND = EntityKinds.FUNCTION_DBT.value

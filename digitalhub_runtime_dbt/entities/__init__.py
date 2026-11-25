# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from digitalhub.plugins import CrudPlugin, EntityPlugin

from digitalhub_runtime_dbt.entities.function.dbt.builder import FunctionDbtBuilder
from digitalhub_runtime_dbt.entities.function.dbt.crud import new_function_dbt
from digitalhub_runtime_dbt.entities.run.transform.builder import RunDbtRunBuilder
from digitalhub_runtime_dbt.entities.task.transform.builder import TaskDbtTransformBuilder

# Imported on first use: the entities register even when the runtime cannot be
# imported, and the import error is reported when a run executes locally.
RUNTIME_DBT_BUILDER = "digitalhub_runtime_dbt.runtimes.builder:RuntimeDbtBuilder"

function_dbt_plugin = EntityPlugin(
    builder=FunctionDbtBuilder,
    shortcuts=(CrudPlugin(new_function_dbt),),
    runtime_builder=RUNTIME_DBT_BUILDER,
)

entity_plugins = (
    function_dbt_plugin,
    EntityPlugin(builder=TaskDbtTransformBuilder, runtime_builder=RUNTIME_DBT_BUILDER),
    EntityPlugin(builder=RunDbtRunBuilder, runtime_builder=RUNTIME_DBT_BUILDER),
)

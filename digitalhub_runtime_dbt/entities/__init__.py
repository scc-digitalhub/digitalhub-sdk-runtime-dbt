# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from digitalhub.factory.plugins import CrudPlugin, EntityPlugin

from digitalhub_runtime_dbt.entities.function.dbt.builder import FunctionDbtBuilder
from digitalhub_runtime_dbt.entities.function.dbt.crud import new_function_dbt
from digitalhub_runtime_dbt.entities.run.transform.builder import RunDbtRunBuilder
from digitalhub_runtime_dbt.entities.task.transform.builder import TaskDbtTransformBuilder

try:
    from digitalhub_runtime_dbt.runtimes.builder import RuntimeDbtBuilder
except ImportError as e:
    from digitalhub.utils.logger.logger import get_logger

    get_logger(__name__).debug(f"Error importing runtime builder: {e}")
    RuntimeDbtBuilder = None

function_dbt_plugin = EntityPlugin(
    builder=FunctionDbtBuilder,
    shortcuts=(CrudPlugin(new_function_dbt),),
    runtime_builder=RuntimeDbtBuilder,
)

entity_plugins = (
    function_dbt_plugin,
    EntityPlugin(builder=TaskDbtTransformBuilder, runtime_builder=RuntimeDbtBuilder),
    EntityPlugin(builder=RunDbtRunBuilder, runtime_builder=RuntimeDbtBuilder),
)

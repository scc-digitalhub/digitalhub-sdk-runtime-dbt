# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from digitalhub.plugins import (
    Action,
    BuilderDef,
    CrudPlugin,
    ExecFamily,
    FunctionBuilder,
    RunBuilder,
    SourceBuilderMixin,
    Status,
    Task,
    TaskBuilder,
    TaskSpecFunction,
)

from digitalhub_runtime_dbt.entities._commons.enums import Actions, EntityKinds
from digitalhub_runtime_dbt.entities.function.dbt.crud import new_function_dbt
from digitalhub_runtime_dbt.entities.function.dbt.entity import FunctionDbt
from digitalhub_runtime_dbt.entities.function.dbt.spec import FunctionSpecDbt
from digitalhub_runtime_dbt.entities.run.transform.entity import RunDbtRun
from digitalhub_runtime_dbt.entities.run.transform.spec import RunSpecDbtRun
from digitalhub_runtime_dbt.entities.run.transform.status import RunStatusDbtRun

dbt = ExecFamily(
    executable=BuilderDef(
        EntityKinds.FUNCTION_DBT,
        FunctionDbt,
        FunctionSpecDbt,
        Status,
        base=FunctionBuilder,
        mixins=(SourceBuilderMixin,),
        shortcuts=(CrudPlugin(new_function_dbt),),
    ),
    actions={
        Actions.TRANSFORM: Action(
            task=BuilderDef(
                EntityKinds.TASK_DBT_TRANSFORM,
                Task,
                TaskSpecFunction,
                Status,
                base=TaskBuilder,
            ),
            run=BuilderDef(
                EntityKinds.RUN_DBT_TRANSFORM,
                RunDbtRun,
                RunSpecDbtRun,
                RunStatusDbtRun,
                base=RunBuilder,
            ),
        ),
    },
    # Imported on first use: the entities register even when the runtime cannot be
    # imported, and the import error is reported when a run executes locally.
    runtime_builder="digitalhub_runtime_dbt.runtimes.builder:RuntimeDbtBuilder",
)

entity_plugins = dbt.plugins()

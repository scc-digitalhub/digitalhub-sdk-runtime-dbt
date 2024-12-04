from __future__ import annotations

from digitalhub.entities.task._base.builder import TaskBuilder

from digitalhub_runtime_dbt.entities._base.runtime_entity.builder import RuntimeEntityBuilderDbt
from digitalhub_runtime_dbt.entities._commons.enums import EntityKinds
from digitalhub_runtime_dbt.entities.task.dbt_transform.entity import TaskDbtTransform
from digitalhub_runtime_dbt.entities.task.dbt_transform.spec import TaskSpecDbtTransform, TaskValidatorDbtTransform
from digitalhub_runtime_dbt.entities.task.dbt_transform.status import TaskStatusDbtTransform


class TaskDbtTransformBuilder(TaskBuilder, RuntimeEntityBuilderDbt):
    """
    TaskDbtTransformBuilder transformer.
    """

    ENTITY_CLASS = TaskDbtTransform
    ENTITY_SPEC_CLASS = TaskSpecDbtTransform
    ENTITY_SPEC_VALIDATOR = TaskValidatorDbtTransform
    ENTITY_STATUS_CLASS = TaskStatusDbtTransform
    ENTITY_KIND = EntityKinds.TASK_DBT_TRANSFORM.value

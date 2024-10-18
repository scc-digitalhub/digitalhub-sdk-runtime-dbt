from __future__ import annotations

from digitalhub_runtime_python.entities._base.runtime_entity.builder import RuntimeEntityBuilderPython
from digitalhub_runtime_python.entities.task.python_build.entity import TaskPythonBuild
from digitalhub_runtime_python.entities.task.python_build.spec import TaskSpecPythonBuild, TaskValidatorPythonBuild
from digitalhub_runtime_python.entities.task.python_build.status import TaskStatusPythonBuild

from digitalhub.entities.task._base.builder import TaskBuilder


class TaskPythonBuildBuilder(TaskBuilder, RuntimeEntityBuilderPython):
    """
    TaskPythonBuild builder.
    """

    ENTITY_CLASS = TaskPythonBuild
    ENTITY_SPEC_CLASS = TaskSpecPythonBuild
    ENTITY_SPEC_VALIDATOR = TaskValidatorPythonBuild
    ENTITY_STATUS_CLASS = TaskStatusPythonBuild
    ENTITY_KIND = "python+build"
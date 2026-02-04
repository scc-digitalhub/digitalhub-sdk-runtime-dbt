# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing

from digitalhub.entities.task._base.entity import Task

if typing.TYPE_CHECKING:
    from digitalhub_runtime_dbt.entities.task.transform.spec import TaskSpecDbtTransform
    from digitalhub_runtime_dbt.entities.task.transform.status import TaskStatusDbtTransform


class TaskDbtTransform(Task):
    """
    TaskDbtTransform class.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.spec: TaskSpecDbtTransform
        self.status: TaskStatusDbtTransform

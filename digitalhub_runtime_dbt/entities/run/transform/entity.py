# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing

from digitalhub.plugins import EntityIORunMixin, LocalExecutionRunMixin, Run

if typing.TYPE_CHECKING:
    from digitalhub_runtime_dbt.entities.run.transform.spec import RunSpecDbtRun
    from digitalhub_runtime_dbt.entities.run.transform.status import RunStatusDbtRun


class RunDbtRun(LocalExecutionRunMixin, EntityIORunMixin, Run):
    """
    RunDbtRun class.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.spec: RunSpecDbtRun
        self.status: RunStatusDbtRun

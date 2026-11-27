# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from digitalhub.plugins import LocalExecutionSpecMixin, RunSpecFunction


class RunSpecDbtRun(RunSpecFunction, LocalExecutionSpecMixin):
    source: dict | None = None
    inputs: dict | None = None
    outputs: dict | None = None
    parameters: dict | None = None

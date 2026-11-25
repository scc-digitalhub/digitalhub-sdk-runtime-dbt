# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing

from digitalhub.plugins import Function, SourceEntityMixin
from digitalhub.utils.file_utils import eval_text_type

if typing.TYPE_CHECKING:
    from digitalhub.plugins import Status

    from digitalhub_runtime_dbt.entities.function.dbt.spec import FunctionSpecDbt


class FunctionDbt(SourceEntityMixin, Function):
    """
    FunctionDbt class.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.spec: FunctionSpecDbt
        self.status: Status

    def _is_inlineable_source(self, path: str) -> bool:
        return eval_text_type(path)

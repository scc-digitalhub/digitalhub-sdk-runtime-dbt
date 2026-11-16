# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum
from typing import ClassVar

from digitalhub.entities._mixin.source.models import HandlerSourceCode
from pydantic import Field


class Lang(Enum):
    """
    Source code language.
    """

    SQL = "sql"
    PYTHON = "python"


class DbtSourceCode(HandlerSourceCode):
    """Source code model for dbt functions."""

    DEFAULT_LANG: ClassVar[str | None] = "sql"
    lang: Lang = Field(default=Lang.SQL.value)


SourceValidator = DbtSourceCode

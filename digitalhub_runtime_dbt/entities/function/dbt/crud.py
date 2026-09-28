# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing

from digitalhub.entities.function.crud import new_function

from digitalhub_runtime_dbt.entities.function.dbt.builder import FunctionDbtBuilder

if typing.TYPE_CHECKING:
    from digitalhub_runtime_dbt.entities.function.dbt.entity import FunctionDbt


def new_function_dbt(
    project: str,
    name: str,
    code: str | None = None,
    code_src: str | None = None,
    source: dict | None = None,
    handler: str | None = None,
    lang: str | None = None,
    uuid: str | None = None,
    version: str | None = None,
    description: str | None = None,
    labels: list[str] | None = None,
    embedded: bool = False,
) -> FunctionDbt:
    """Create a dbt function entity."""
    if code is not None and code_src is not None:
        raise ValueError("Only one of 'code' or 'code_src' can be provided.")

    return new_function(
        project=project,
        name=name,
        kind=FunctionDbtBuilder.ENTITY_KIND,
        uuid=uuid,
        version=version,
        description=description,
        labels=labels,
        embedded=embedded,
        code=code,
        code_src=code_src,
        source=source,
        handler=handler,
        lang=lang,
    )

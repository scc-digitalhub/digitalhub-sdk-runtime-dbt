# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt.cli.main import dbtRunnerResult


def transform(output: str, root: Path) -> dbtRunnerResult:
    """
    Execute a dbt project with the specified outputs.
    It initializes a dbt runner, cleans the project and runs it.

    Parameters
    ----------
    output : str
        The output table name.

    Returns
    -------
    dbtRunnerResult
        An object representing the result of the dbt execution.
    """
    from dbt.cli.main import dbtRunner

    current_dir = os.getcwd()
    os.chdir(root)
    dbt = dbtRunner()
    dbt.invoke("clean")
    cli_args = ["run", "--select", f"{output}"]
    res = dbt.invoke(cli_args)
    os.chdir(current_dir)
    return res

# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from plugin_contract import check_plugin_package

from digitalhub_runtime_dbt.entities._commons.enums import EntityKinds


def test_runtime_honours_the_plugin_contract() -> None:
    check_plugin_package("digitalhub_runtime_dbt", kinds=EntityKinds)

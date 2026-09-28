# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0
from digitalhub_runtime_dbt.entities import entity_plugins
from digitalhub_runtime_dbt.entities._commons.enums import EntityKinds

entity_builders = tuple((plugin.kind, plugin.builder) for plugin in entity_plugins)

try:
    from digitalhub_runtime_dbt.runtimes.builder import RuntimeDbtBuilder

    runtime_builders = tuple((kind.value, RuntimeDbtBuilder) for kind in EntityKinds)
except ImportError as e:
    from digitalhub.utils.logger.logger import get_logger

    logger = get_logger(__name__)
    logger.debug(f"Error importing runtime builders: {e}")
    runtime_builders = tuple()

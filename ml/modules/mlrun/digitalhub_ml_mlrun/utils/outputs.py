from __future__ import annotations

import typing

from pathlib import Path
from digitalhub_core.entities._base.status import State
from digitalhub_core.entities.artifacts.crud import create_artifact
from digitalhub_core.utils.logger import LOGGER
from digitalhub_data.entities.dataitems.crud import create_dataitem
from digitalhub_data.utils.data_utils import get_data_preview
from digitalhub_core.utils.uri_utils import map_uri_scheme

if typing.TYPE_CHECKING:
    from digitalhub_core.entities.artifacts.entity import Artifact
    from digitalhub_data.entities.dataitems.entity._base import Dataitem
    from digitalhub_data.entities.dataitems.entity.table import DataitemTable
    from digitalhub_ml.entities.models.entity import Model
    from mlrun.runtimes.base import RunObject, BuildStatus


def map_state(state: str) -> str:
    """
    Map Mlrun state to digitalhub state.

    Parameters
    ----------
    state : str
        Mlrun state.

    Returns
    -------
    str
        Mapped digitalhub state.
    """
    mlrun_states = {
        "completed": State.COMPLETED.value,
        "error": State.ERROR.value,
        "running": State.RUNNING.value,
        "created": State.CREATED.value,
        "pending": State.PENDING.value,
        "unknown": State.ERROR.value,
        "aborted": State.STOP.value,
        "aborting": State.STOP.value,
    }
    return mlrun_states.get(state, State.ERROR.value)


def parse_mlrun_artifacts(mlrun_outputs: list[dict], project: str) -> list[Artifact|Dataitem]:
    """
    Filter out models and datasets from Mlrun outputs and create DHCore entities.

    Parameters
    ----------
    project : str
        DHCore project name.
    mlrun_outputs : list[dict]
        Mlrun outputs.

    Returns
    -------
    list[Artifact|Dataitem]
        List of artifacts and datasets.
    """
    outputs = []
    for i in mlrun_outputs:
        if i.get("kind") == "model":
            ...
        elif i.get("kind") == "dataset":
            outputs.append(_create_dataitem(project, i))
        else:
            outputs.append(_create_artifact(project, i))
    return outputs


def _create_artifact(project: str, mlrun_artifact: dict) -> Artifact:
    """
    New artifact.

    Parameters
    ----------
    project : str
        DHCore project name.
    mlrun_artifact : dict
        Mlrun artifact.

    Returns
    -------
    Artifact
        Artifact object.
    """
    try:
        kwargs = {}
        kwargs["project"] = project
        kwargs["name"] = mlrun_artifact.get("metadata", {}).get("key")
        kwargs["kind"] = "artifact"
        kwargs["path"] = mlrun_artifact.get("spec", {}).get("target_path")
        kwargs["size"] = mlrun_artifact.get("spec", {}).get("size")
        kwargs["hash"] = mlrun_artifact.get("spec", {}).get("hash")

        artifact: Artifact = create_artifact(**kwargs)

        # Upload artifact if Mlrun artifact is local
        if map_uri_scheme(artifact.spec.path) == "local":
            filename = Path(artifact.spec.path).name
            src_path = artifact.spec.path
            artifact.spec.path = f"s3://datalake/{project}/artifacts/artifact/{filename}"
            artifact.upload(src=src_path)

        artifact.save()
        return artifact

    except Exception:
        msg = "Something got wrong during artifact creation."
        LOGGER.exception(msg)
        raise RuntimeError(msg)


def _create_dataitem(project: str, mlrun_output: dict) -> Dataitem:
    """
    New dataitem.

    Parameters
    ----------
    project : str
        DHCore project name.
    mlrun_output : dict
        Mlrun output.

    Returns
    -------
    Dataitem
        Dataitem object.
    """
    try:
        # Create dataitem
        kwargs = {}
        kwargs["project"] = project
        kwargs["name"] = mlrun_output.get("metadata", {}).get("key")
        kwargs["kind"] = "table"
        kwargs["path"] = mlrun_output.get("spec", {}).get("target_path")
        kwargs["schema"] = mlrun_output.get("spec", {}).get("schema", {})

        dataitem: DataitemTable = create_dataitem(**kwargs)

        # Check on path. If mlrun output is local, write data to minio
        if map_uri_scheme(dataitem.spec.path) == "local":
            target_path = f"s3://datalake/{project}/dataitems/table/{dataitem.name}.parquet"
            new_path = dataitem.write_df(target_path=target_path)
            dataitem.spec.path = new_path

        # Add sample preview
        header = mlrun_output.get("spec", {}).get("header", [])
        sample_data = mlrun_output.get("status", {}).get("preview", [[]])
        data_preview = _get_data_preview(header, sample_data)
        rows = mlrun_output.get("spec", {}).get("lenght")
        dataitem.status.preview = {
            "cols": data_preview,
            "rows": rows,
        }

        # Save dataitem in core and return it
        dataitem.save()
        return dataitem
    except Exception:
        msg = "Something got wrong during dataitem creation."
        LOGGER.exception(msg)
        raise RuntimeError(msg)


def _get_data_preview(columns: tuple, data: list[tuple]) -> list[dict]:
    """
    Get data preview from dbt result.

    Parameters
    ----------
    columns : tuple
        The columns.
    data : list[tuple]
        The data.

    Returns
    -------
    list
        A list of dictionaries containing data.
    """
    try:
        return get_data_preview(columns, data)
    except Exception:
        msg = "Something got wrong during data preview creation."
        LOGGER.exception(msg)
        raise RuntimeError(msg)


def build_status(
    execution_results: RunObject,
    entity_outputs: list[Artifact | Dataitem | Model],
    mapped_outputs: dict = None,
    values_list: list = None,
) -> dict:
    """
    Collect outputs.

    Parameters
    ----------
    execution_results : RunObject
        Execution results.
    outputs : list[Artifact | Dataitem | Model]
        List of entities to collect outputs from.
    mapped_outputs : dict
        Mapped outputs.
    values_list : list
        Values list.

    Returns
    -------
    dict
        Status dict.
    """
    # Map outputs
    outputs = []

    execution_outputs_keys = [k for k, _ in execution_results.outputs.items()]
    if mapped_outputs is not None:
        for i in mapped_outputs:
            for k, v in i.items():
                if k in execution_outputs_keys:
                    for j in entity_outputs:
                        if j.name == k:
                            outputs.append({v: j.key})
    else:
        outputs = [{i.name: i.key} for i in entity_outputs]

    # Map results and values
    results = {}

    if values_list is not None:
        for i in values_list:
            if i in execution_outputs_keys:
                results[i] = [v for k, v in execution_results.outputs.items() if k == i][0]
    else:
        for i, j in execution_results.outputs.items():
            results[i] = j

    results["mlrun_result"] = execution_results.to_json()

    try:
        return {
            "state": State.COMPLETED.value,
            "outputs": outputs,
            "results": results,
        }
    except Exception:
        msg = "Something got wrong during run status building."
        LOGGER.exception(msg)
        raise RuntimeError(msg)


def build_status_build(
    execution_results: BuildStatus
) -> dict:
    """
    Collect outputs.

    Parameters
    ----------
    execution_results : BuildStatus
        Build results.

    Returns
    -------
    dict
        Status dict.
    """

    # Map results and values
    results = {}

    results["mlrun_result"] = execution_results.to_json()
    results["target_image"] = execution_results.outputs.image

    try:
        return {
            "state": State.COMPLETED.value,
            "results": results,
        }
    except Exception:
        msg = "Something got wrong during run status building."
        LOGGER.exception(msg)
        raise RuntimeError(msg)
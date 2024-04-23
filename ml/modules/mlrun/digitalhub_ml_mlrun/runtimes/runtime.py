"""
Runtime class for running Mlrun functions.
"""
from __future__ import annotations

import shutil
import typing
from pathlib import Path
from typing import Callable

from digitalhub_core.runtimes.base import Runtime
from digitalhub_core.utils.logger import LOGGER
from digitalhub_ml_mlrun.utils.configurations import (
    get_dhcore_function,
    get_mlrun_function,
    get_mlrun_project,
    parse_function_specs,
    save_function_source,
)
from digitalhub_ml_mlrun.utils.functions import run_job, run_build
from digitalhub_ml_mlrun.utils.inputs import get_inputs_parameters
from digitalhub_ml_mlrun.utils.outputs import build_status, parse_mlrun_artifacts, build_status_build

if typing.TYPE_CHECKING:
    from mlrun.runtimes import BaseRuntime
    from mlrun.runtimes.base import RunObject, BuildStatus


class RuntimeMlrun(Runtime):
    """
    Runtime Mlrun class.
    """

    allowed_actions = ["job", "build"]

    def __init__(self) -> None:
        """
        Constructor.
        """
        super().__init__()

        self.root_path = Path("/tmp/mlrun_run")
        self.tmp_path = self.root_path / "temp"
        self.function_source = None

        self.root_path.mkdir(parents=True, exist_ok=True)

    def build(self, function: dict, task: dict, run: dict) -> dict:
        """
        Build run spec.

        Parameters
        ----------
        function : dict
            The function.
        task : dict
            The task.
        run : dict
            The run.

        Returns
        -------
        dict
            The run spec.
        """
        task_kind = task.get("kind").split("+")[1]
        return {
            "function_spec": function.get("spec", {}),
            f"{task_kind}_spec": task.get("spec", {}),
            **run.get("spec", {}),
        }

    def run(self, run: dict) -> dict:
        """
        Run function.

        Returns
        -------
        dict
            Status of the executed run.
        """
        LOGGER.info("Validating task.")
        action = self._validate_task(run)
        executable = self._get_executable(action)

        LOGGER.info("Starting task.")
        spec = run.get("spec")
        project = run.get("project")

        LOGGER.info("Collecting inputs.")
        function_args = self._collect_inputs(spec)

        LOGGER.info("Configure execution.")
        mlrun_function, exec_config = self._configure_execution(spec, action, project)

        LOGGER.info("Executing function.")
        results = self._execute(executable, mlrun_function, exec_config, function_args)

        LOGGER.info("Collecting outputs.")
        status = self._collect_outputs(results, spec, project)

        LOGGER.info("Cleanup")
        self._cleanup()

        LOGGER.info("Task completed, returning run status.")
        return status

    @staticmethod
    def _get_executable(action: str) -> Callable:
        """
        Select function according to action.

        Parameters
        ----------
        action : str
            Action to execute.

        Returns
        -------
        Callable
            Function to execute.
        """
        if action == "job":
            return run_job
        if action == "build":
            return run_build
        raise NotImplementedError

    ####################
    # Helpers
    ####################

    def _collect_inputs(self, spec: dict) -> dict:
        """
        Collect inputs.

        Parameters
        ----------
        spec : dict
            Run specs.
        project : str
            Project name.

        Returns
        -------
        dict
            Parameters.
        """
        LOGGER.info("Getting inputs.")
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        return get_inputs_parameters(spec.get("inputs", []), spec.get("parameters", {}), self.tmp_path)

    ####################
    # Configuration
    ####################

    def _configure_execution(self, spec: dict, action: str, project: str) -> tuple[BaseRuntime, dict]:
        """
        Create Mlrun project and function and prepare parameters.

        Parameters
        ----------
        spec : dict
            Run specs.
        action : str
            Action to execute.
        project : str
            Project name.

        Returns
        -------
        tuple
            Mlrun function and parameters.
        """

        # Setup function source and specs
        LOGGER.info("Getting function source and specs.")
        dhcore_function = get_dhcore_function(spec.get(f"{action}_spec", {}).get("function"))
        function_source = save_function_source(self.tmp_path, dhcore_function.spec.to_dict().get("source"))
        function_specs = parse_function_specs(dhcore_function.spec.to_dict())

        # Create Mlrun project
        LOGGER.info("Creating Mlrun project and function.")
        mlrun_project = get_mlrun_project(project)

        mlrun_function = get_mlrun_function(mlrun_project, dhcore_function.name, function_source, function_specs)
        exec_config = {}
        if action == "build":
            task_spec = spec.get(f"{action}_spec")
            target_image = task_spec.get("target_image")
            commands = task_spec.get("commands")
            force_build = task_spec.get("force_build")
            if target_image is not None: exec_config["target_image"] = target_image
            if commands is not None: exec_config["commands"] = commands
            if force_build is not None: exec_config["force_build"] = force_build
        return mlrun_function, exec_config


    ####################
    # Outputs
    ####################

    def _collect_outputs(self, results: RunObject | BuildStatus, spec: dict, project: str) -> dict:
        """
        Collect outputs.

        Parameters
        ----------
        results : RunObject | BuildStatus
            Execution results.
        spec : dict
            Run specs.
        project : str
            Project name.

        Returns
        -------
        dict
            Status of the executed run.
        """
        if isinstance(results, RunObject):
            execution_outputs = parse_mlrun_artifacts(results.status.artifacts, project)
            return build_status(results, execution_outputs, spec.get("outputs", []), spec.get("values", []))
        elif isinstance(results, BuildStatus):
            return build_status_build(results)
        raise NotImplementedError

    ####################
    # Cleanup
    ####################

    def _cleanup(self) -> None:
        """
        Cleanup root folder.

        Returns
        -------
        None
        """
        shutil.rmtree(self.root_path, ignore_errors=True)

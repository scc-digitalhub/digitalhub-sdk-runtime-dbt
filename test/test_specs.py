import pytest
from digitalhub.entities._constructors.spec import build_spec
from pydantic import ValidationError

from digitalhub_runtime_dbt.entities.function.dbt.models import SourceValidator
from digitalhub_runtime_dbt.entities.function.dbt.spec import FunctionSpecDbt
from digitalhub_runtime_dbt.entities.run.transform.spec import RunSpecDbtRun
from digitalhub_runtime_dbt.entities.task.transform.spec import TaskSpecDbtTransform


def test_dbt_function_spec_validates_nested_source():
    spec = build_spec(FunctionSpecDbt, source={"source": "project", "lang": "sql"})

    assert isinstance(spec.source, SourceValidator)
    assert spec.source.lang == "sql"

    with pytest.raises(ValidationError):
        build_spec(FunctionSpecDbt)


def test_dbt_task_and_run_specs_keep_their_own_fields():
    task = build_spec(TaskSpecDbtTransform, function="function", profile="dbt")
    run = build_spec(
        RunSpecDbtRun,
        task="task",
        source={"source": "models"},
        inputs={"input": "value"},
        outputs={"output": "value"},
        parameters={"threads": 2},
        local_execution=True,
    )

    assert task.function == "function"
    assert run.source == {"source": "models"}
    assert run.inputs == {"input": "value"}
    assert run.outputs == {"output": "value"}
    assert run.parameters == {"threads": 2}
    assert run.local_execution is True

    with pytest.raises(TypeError, match="instructions"):
        build_spec(RunSpecDbtRun, task="task", instructions=["dbt run"])

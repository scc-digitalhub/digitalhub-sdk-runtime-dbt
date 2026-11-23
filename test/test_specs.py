import pytest
from digitalhub.entities._constructors.spec import build_spec
from digitalhub.entities.task._base.spec import TaskSpecFunction
from pydantic import ValidationError

from digitalhub_runtime_dbt.entities.function.dbt.builder import FunctionDbtBuilder
from digitalhub_runtime_dbt.entities.function.dbt.models import DbtSourceCode
from digitalhub_runtime_dbt.entities.function.dbt.spec import FunctionSpecDbt
from digitalhub_runtime_dbt.entities.run.transform.spec import RunSpecDbtRun


def test_dbt_function_spec_validates_nested_source():
    spec = build_spec(FunctionSpecDbt, source={"source": "project", "lang": "sql"})

    assert isinstance(spec.source, DbtSourceCode)
    assert spec.source.lang == "sql"

    with pytest.raises(ValidationError):
        build_spec(FunctionSpecDbt)


def test_dbt_builder_routes_top_level_source_arguments():
    builder = FunctionDbtBuilder()

    function = builder.build(
        kind=builder.ENTITY_KIND,
        project="project",
        name="function",
        code="select 1",
    )

    assert function.spec.source.base64 == "c2VsZWN0IDE="
    assert function.spec.source.lang == "sql"


def test_dbt_task_and_run_specs_keep_their_own_fields():
    task = build_spec(TaskSpecFunction, function="function", profile="dbt")
    run = build_spec(
        RunSpecDbtRun,
        function="function",
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
        build_spec(RunSpecDbtRun, function="function", task="task", instructions=["dbt run"])

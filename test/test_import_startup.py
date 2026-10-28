import subprocess
import sys
import textwrap


def test_importing_runtime_defers_dbt_cli() -> None:
    script = textwrap.dedent(
        """
        import sys

        import digitalhub_runtime_dbt

        assert "dbt.cli.main" not in sys.modules
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_transform_loads_and_invokes_dbt_runner(tmp_path) -> None:
    script = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        from types import ModuleType

        invocations = []

        class FakeDbtRunner:
            def invoke(self, args):
                invocations.append(args)
                return args

        dbt = ModuleType("dbt")
        dbt.__path__ = []
        cli = ModuleType("dbt.cli")
        cli.__path__ = []
        main = ModuleType("dbt.cli.main")
        main.dbtRunner = FakeDbtRunner
        sys.modules.update({"dbt": dbt, "dbt.cli": cli, "dbt.cli.main": main})

        from digitalhub_runtime_dbt.utils.functions import transform

        result = transform("model_name", Path(sys.argv[1]))

        assert invocations == ["clean", ["run", "--select", "model_name"]]
        assert result == ["run", "--select", "model_name"]
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stderr

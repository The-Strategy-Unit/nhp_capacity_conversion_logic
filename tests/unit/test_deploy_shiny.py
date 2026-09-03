import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import call

import pytest


def _load_deployment_script() -> ModuleType:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "deploy_shiny.py"
    spec = importlib.util.spec_from_file_location("deploy_shiny", script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load deployment script from {script_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


deploy = _load_deployment_script()


def test_project_root_is_repository_root() -> None:
    assert deploy.PROJECT_ROOT == Path(__file__).resolve().parents[2]


def test_deployment_bundle_includes_favicon() -> None:
    assert "www/favicon.ico" in deploy.BUNDLE_FILES


@pytest.mark.parametrize(
    ("choice", "expected"),
    [
        ("1", deploy.DeploymentType.NEW),
        ("new", deploy.DeploymentType.NEW),
        ("2", deploy.DeploymentType.REDEPLOY),
        ("redeploy", deploy.DeploymentType.REDEPLOY),
        ("q", None),
    ],
)
def test_choose_deployment_type(
    monkeypatch: pytest.MonkeyPatch,
    choice: str,
    expected: object,
) -> None:
    monkeypatch.setattr("builtins.input", lambda _: choice)

    assert deploy.choose_deployment_type() is expected


def test_choose_deployment_type_reprompts_after_invalid_input(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    choices = iter(["invalid", "1"])
    monkeypatch.setattr("builtins.input", lambda _: next(choices))

    assert deploy.choose_deployment_type() is deploy.DeploymentType.NEW
    assert "Enter 1, 2, or q." in capsys.readouterr().out


def _configure_valid_preflight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(deploy, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(deploy, "ENV_FILE", tmp_path / ".env")
    monkeypatch.setattr(deploy, "BUNDLE_FILES", ())
    monkeypatch.setattr(deploy, "CAPACITY_SOURCE_GLOB", "*.py")
    (tmp_path / "capacity.py").touch()
    monkeypatch.setattr(deploy.shutil, "which", lambda _: "/path/to/tool")

    for env_var in (*deploy.CONNECT_ENV_VARS, *deploy.RUNTIME_ENV_VARS):
        monkeypatch.setenv(env_var, "configured")
    monkeypatch.setenv(
        "FEEDBACK_FORM_URL",
        "https://forms.example.test/feedback",
    )


def _current_environment_sources() -> dict[str, object]:
    return {
        name: deploy.EnvironmentSource.CURRENT_ENVIRONMENT
        for name in deploy.DEPLOYMENT_ENV_VARS
        if name in deploy.os.environ
    }


def test_new_deployment_does_not_require_app_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_valid_preflight(monkeypatch, tmp_path)
    monkeypatch.delenv("CONNECT_APP_ID", raising=False)

    checks = {
        check.label: check
        for check in deploy.collect_preflight_checks(
            deploy.DeploymentType.NEW,
            _current_environment_sources(),
        )
    }

    assert "CONNECT_APP_ID" not in checks
    assert all(check.passed for check in checks.values() if check.required)
    assert checks[".env file"].required is False


def test_new_deployment_requires_feedback_form_url(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_valid_preflight(monkeypatch, tmp_path)
    monkeypatch.delenv("FEEDBACK_FORM_URL")

    checks = {
        check.label: check
        for check in deploy.collect_preflight_checks(
            deploy.DeploymentType.NEW,
            _current_environment_sources(),
        )
    }

    assert checks["FEEDBACK_FORM_URL"].passed is False
    assert checks["FEEDBACK_FORM_URL"].required is True
    assert checks["FEEDBACK_FORM_URL"].source is deploy.EnvironmentSource.UNSET
    assert "AZ_FUNC_AGG_GUID" not in deploy.RUNTIME_ENV_VARS


def test_dotenv_feedback_url_overrides_stale_current_environment_value(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_valid_preflight(monkeypatch, tmp_path)
    dotenv_value = "https://forms.example.test/feedback"
    (tmp_path / ".env").write_text(
        f"FEEDBACK_FORM_URL={dotenv_value}\n",
        encoding="utf-8",
    )
    stale_value = "not-a-valid-url"
    monkeypatch.setenv("FEEDBACK_FORM_URL", stale_value)

    environment_sources = deploy.load_deployment_environment()
    checks = {
        check.label: check
        for check in deploy.collect_preflight_checks(
            deploy.DeploymentType.NEW,
            environment_sources,
        )
    }
    feedback_check = checks["FEEDBACK_FORM_URL"]

    assert deploy.os.environ["FEEDBACK_FORM_URL"] == dotenv_value
    assert feedback_check.passed is True
    assert feedback_check.source is deploy.EnvironmentSource.DOTENV

    deploy.print_preflight_checks([feedback_check])
    output = capsys.readouterr().out
    assert "OK      FEEDBACK_FORM_URL (source: .env)" in output
    assert stale_value not in output
    assert dotenv_value not in output


def test_preflight_rejects_invalid_feedback_url_from_current_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_valid_preflight(monkeypatch, tmp_path)
    invalid_value = "not-a-valid-url"
    monkeypatch.setenv("FEEDBACK_FORM_URL", invalid_value)

    environment_sources = deploy.load_deployment_environment()
    feedback_check = deploy._environment_preflight_check(
        "FEEDBACK_FORM_URL",
        environment_sources,
    )

    assert feedback_check.passed is False
    assert feedback_check.failure_status == "INVALID"
    assert feedback_check.source is deploy.EnvironmentSource.CURRENT_ENVIRONMENT

    deploy.print_preflight_checks([feedback_check])
    output = capsys.readouterr().out
    assert "INVALID FEEDBACK_FORM_URL (source: current environment)" in output
    assert "must be a valid HTTPS URL" in output
    assert invalid_value not in output


def test_preflight_reports_feedback_url_loaded_from_dotenv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(deploy, "ENV_FILE", tmp_path / ".env")
    (tmp_path / ".env").write_text(
        "FEEDBACK_FORM_URL=https://forms.example.test/feedback\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("FEEDBACK_FORM_URL", raising=False)

    environment_sources = deploy.load_deployment_environment()
    feedback_check = deploy._environment_preflight_check(
        "FEEDBACK_FORM_URL",
        environment_sources,
    )

    assert feedback_check.passed is True
    assert feedback_check.source is deploy.EnvironmentSource.DOTENV

    deploy.print_preflight_checks([feedback_check])
    assert "OK      FEEDBACK_FORM_URL (source: .env)" in capsys.readouterr().out


def test_redeployment_requires_app_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_valid_preflight(monkeypatch, tmp_path)
    monkeypatch.delenv("CONNECT_APP_ID", raising=False)

    checks = {
        check.label: check
        for check in deploy.collect_preflight_checks(
            deploy.DeploymentType.REDEPLOY,
            _current_environment_sources(),
        )
    }

    assert checks["CONNECT_APP_ID"].passed is False
    assert checks["CONNECT_APP_ID"].required is True


def test_preflight_reports_missing_tools_and_bundle_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(deploy, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(deploy, "ENV_FILE", tmp_path / ".env")
    monkeypatch.setattr(deploy, "BUNDLE_FILES", ("app.py",))
    monkeypatch.setattr(deploy, "CAPACITY_SOURCE_GLOB", "*.py")
    monkeypatch.setattr(deploy.shutil, "which", lambda _: None)

    checks = {
        check.label: check
        for check in deploy.collect_preflight_checks(deploy.DeploymentType.NEW, {})
    }

    assert checks["uv CLI"].passed is False
    assert checks["rsconnect CLI"].passed is False
    assert checks["deployment bundle files"].passed is False
    assert checks["capacity-conversion source files"].passed is False


@pytest.mark.parametrize(
    ("deployment_type", "expected_option"),
    [
        (deploy.DeploymentType.NEW, ["--new"]),
        (deploy.DeploymentType.REDEPLOY, ["--app-id", "app-guid"]),
    ],
)
def test_build_deploy_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    deployment_type: object,
    expected_option: list[str],
) -> None:
    source_directory = tmp_path / "src" / "nhp" / "capacity_conversion"
    source_directory.mkdir(parents=True)
    source_file = source_directory / "capacity.py"
    source_file.touch()
    (tmp_path / "app.py").touch()

    monkeypatch.setattr(deploy, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(deploy, "BUNDLE_FILES", ("app.py",))
    monkeypatch.setattr(
        deploy,
        "CAPACITY_SOURCE_GLOB",
        "src/nhp/capacity_conversion/*.py",
    )
    monkeypatch.setenv("CONNECT_APP_ID", "app-guid")
    monkeypatch.setenv("CONNECT_API_KEY", "secret-api-key")
    for env_var in deploy.RUNTIME_ENV_VARS:
        monkeypatch.setenv(env_var, "configured")

    command = deploy.build_deploy_command(deployment_type, "rsconnect")

    assert command[:3] == ["rsconnect", "deploy", "shiny"]
    assert command[3:5] == [
        "--title",
        "OpenPlan Capacity Conversion Model (development)",
    ]
    assert all(option in command for option in expected_option)
    assert "secret-api-key" not in command
    exclude_index = command.index("--exclude=**")
    assert command[exclude_index + 1] == "."
    assert command[-2:] == [
        "app.py",
        "src/nhp/capacity_conversion/capacity.py",
    ]
    for env_var in deploy.RUNTIME_ENV_VARS:
        assert ["-E", env_var] == command[
            command.index(env_var) - 1 : command.index(env_var) + 1
        ]


def test_confirm_deployment_does_not_print_api_key(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CONNECT_SERVER", "https://connect.example.test")
    monkeypatch.setenv("CONNECT_API_KEY", "secret-api-key")
    monkeypatch.setattr("builtins.input", lambda _: "yes")

    assert deploy.confirm_deployment(deploy.DeploymentType.NEW) is True
    assert "secret-api-key" not in capsys.readouterr().out


def test_main_checks_connection_then_deploys(mocker) -> None:
    mocker.patch.dict(
        deploy.os.environ,
        {"CONNECT_SERVER": "https://connect.example.test"},
    )
    mocker.patch.object(deploy, "load_deployment_environment", return_value={})
    mocker.patch.object(
        deploy,
        "choose_deployment_type",
        return_value=deploy.DeploymentType.NEW,
    )
    mocker.patch.object(
        deploy,
        "collect_preflight_checks",
        return_value=[deploy.PreflightCheck("ready", True)],
    )
    mocker.patch.object(deploy, "print_preflight_checks")
    mocker.patch.object(deploy, "confirm_deployment", return_value=True)
    mocker.patch.object(deploy.shutil, "which", return_value="/bin/rsconnect")
    mocker.patch.object(
        deploy,
        "build_deploy_command",
        return_value=["/bin/rsconnect", "deploy"],
    )
    run_command = mocker.patch.object(deploy, "run_command", side_effect=[0, 0])

    assert deploy.main() == 0
    assert run_command.call_args_list == [
        call(
            [
                "/bin/rsconnect",
                "details",
                "--server",
                "https://connect.example.test",
            ]
        ),
        call(["/bin/rsconnect", "deploy"]),
    ]


def test_main_stops_when_connection_check_fails(mocker) -> None:
    mocker.patch.dict(
        deploy.os.environ,
        {"CONNECT_SERVER": "https://connect.example.test"},
    )
    mocker.patch.object(deploy, "load_deployment_environment", return_value={})
    mocker.patch.object(
        deploy,
        "choose_deployment_type",
        return_value=deploy.DeploymentType.NEW,
    )
    mocker.patch.object(
        deploy,
        "collect_preflight_checks",
        return_value=[deploy.PreflightCheck("ready", True)],
    )
    mocker.patch.object(deploy, "print_preflight_checks")
    mocker.patch.object(deploy, "confirm_deployment", return_value=True)
    mocker.patch.object(deploy.shutil, "which", return_value="/bin/rsconnect")
    run_command = mocker.patch.object(deploy, "run_command", return_value=1)

    assert deploy.main() == 1
    run_command.assert_called_once_with(
        [
            "/bin/rsconnect",
            "details",
            "--server",
            "https://connect.example.test",
        ]
    )

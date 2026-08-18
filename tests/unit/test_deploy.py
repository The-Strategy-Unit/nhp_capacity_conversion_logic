from pathlib import Path
from unittest.mock import call

import pytest

from nhp import deploy


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
    expected: deploy.DeploymentType | None,
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


def test_new_deployment_does_not_require_app_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_valid_preflight(monkeypatch, tmp_path)
    monkeypatch.delenv("CONNECT_APP_ID", raising=False)

    checks = {
        check.label: check
        for check in deploy.collect_preflight_checks(deploy.DeploymentType.NEW)
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
        for check in deploy.collect_preflight_checks(deploy.DeploymentType.NEW)
    }

    assert checks["FEEDBACK_FORM_URL"].passed is False
    assert checks["FEEDBACK_FORM_URL"].required is True


def test_redeployment_requires_app_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_valid_preflight(monkeypatch, tmp_path)
    monkeypatch.delenv("CONNECT_APP_ID", raising=False)

    checks = {
        check.label: check
        for check in deploy.collect_preflight_checks(deploy.DeploymentType.REDEPLOY)
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
        for check in deploy.collect_preflight_checks(deploy.DeploymentType.NEW)
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
    deployment_type: deploy.DeploymentType,
    expected_option: list[str],
) -> None:
    monkeypatch.setattr(deploy, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(deploy, "BUNDLE_FILES", ("app.py",))
    monkeypatch.setattr(deploy, "CAPACITY_SOURCE_GLOB", "*.py")
    monkeypatch.setenv("CONNECT_APP_ID", "app-guid")
    monkeypatch.setenv("CONNECT_API_KEY", "secret-api-key")
    (tmp_path / "app.py").touch()
    (tmp_path / "capacity.py").touch()

    command = deploy.build_deploy_command(deployment_type, "rsconnect")

    assert command[:3] == ["rsconnect", "deploy", "shiny"]
    assert all(option in command for option in expected_option)
    assert "secret-api-key" not in command
    exclude_index = command.index("--exclude=**")
    assert command[exclude_index + 1] == "."
    assert command[-2:] == ["app.py", "capacity.py"]
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
    mocker.patch.object(deploy, "load_dotenv")
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
    mocker.patch.object(deploy, "load_dotenv")
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

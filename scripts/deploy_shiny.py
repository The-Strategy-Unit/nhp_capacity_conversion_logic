"""Interactive deployment helper for the Shiny application."""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from urllib.parse import urlparse

from dotenv import dotenv_values, load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
APP_TITLE = "NHP Capacity Conversion (development)"

CONNECT_ENV_VARS = (
    "CONNECT_SERVER",
    "CONNECT_API_KEY",
)
RUNTIME_ENV_VARS = (
    "AZ_FUNC_AGG_GUID",
    "AZ_STORAGE_EP",
    "AZ_STORAGE_RESULTS",
    "AZ_TABLE_ENDPOINT",
    "FEEDBACK_FORM_URL",
    "TABLE_NAME",
)
BUNDLE_FILES = (
    "app.py",
    "_brand.yml",
    "README.md",
    "pyproject.toml",
    "requirements.txt",
    "src/nhp/__init__.py",
)
CAPACITY_SOURCE_GLOB = "src/nhp/capacity_conversion/*.py"
DEPLOYMENT_ENV_VARS = (*CONNECT_ENV_VARS, *RUNTIME_ENV_VARS, "CONNECT_APP_ID")


class DeploymentType(StrEnum):
    """Supported Posit Connect deployment operations."""

    NEW = "new"
    REDEPLOY = "redeploy"


class EnvironmentSource(StrEnum):
    """Where an effective deployment environment variable came from."""

    CURRENT_ENVIRONMENT = "current environment"
    DOTENV = ".env"
    UNSET = "not configured"


@dataclass(frozen=True)
class PreflightCheck:
    """One deployment prerequisite and its diagnostic result."""

    label: str
    passed: bool
    detail: str = ""
    required: bool = True
    source: EnvironmentSource | None = None
    failure_status: str = "MISSING"


def load_deployment_environment() -> dict[str, EnvironmentSource]:
    """Load `.env` with precedence and record each effective value's source."""
    inherited_environment = set(os.environ)
    dotenv_environment = dotenv_values(dotenv_path=ENV_FILE)
    load_dotenv(dotenv_path=ENV_FILE, override=True)

    return {
        name: (
            EnvironmentSource.DOTENV
            if dotenv_environment.get(name) is not None
            else EnvironmentSource.CURRENT_ENVIRONMENT
            if name in inherited_environment
            else EnvironmentSource.UNSET
        )
        for name in DEPLOYMENT_ENV_VARS
    }


def _environment_preflight_check(
    name: str,
    environment_sources: Mapping[str, EnvironmentSource],
) -> PreflightCheck:
    """Validate one required deployment environment variable."""
    value = os.getenv(name, "").strip()
    source = environment_sources.get(name, EnvironmentSource.UNSET)
    if name == "FEEDBACK_FORM_URL" and value:
        feedback_url = urlparse(value)
        if feedback_url.scheme != "https" or not feedback_url.netloc:
            return PreflightCheck(
                label=name,
                passed=False,
                detail="must be a valid HTTPS URL",
                source=source,
                failure_status="INVALID",
            )

    return PreflightCheck(
        label=name,
        passed=bool(value),
        detail="set it in .env or the current environment",
        source=source,
    )


def choose_deployment_type() -> DeploymentType | None:
    """Prompt until the developer chooses a deployment operation or cancels."""
    print("Deployment type:")
    print("  1. Deploy a new app")
    print("  2. Redeploy an existing app")
    print("  q. Cancel")

    while True:
        try:
            choice = input("Select an option: ").strip().casefold()
        except (EOFError, KeyboardInterrupt):
            print()
            return None

        if choice in {"1", "new"}:
            return DeploymentType.NEW
        if choice in {"2", "redeploy"}:
            return DeploymentType.REDEPLOY
        if choice in {"q", "quit", "cancel"}:
            return None

        print("Enter 1, 2, or q.")


def collect_preflight_checks(
    deployment_type: DeploymentType,
    environment_sources: Mapping[str, EnvironmentSource],
) -> list[PreflightCheck]:
    """Check local tools, bundle inputs, and deployment configuration."""
    checks = [
        PreflightCheck(
            label=".env file",
            passed=ENV_FILE.is_file(),
            detail="using variables from the current environment",
            required=False,
        ),
        PreflightCheck(
            label="uv CLI",
            passed=shutil.which("uv") is not None,
            detail="install uv: https://docs.astral.sh/uv/getting-started/installation/",
        ),
        PreflightCheck(
            label="rsconnect CLI",
            passed=shutil.which("rsconnect") is not None,
            detail=(
                "run this command with: "
                "uv run --locked --group dev scripts/deploy_shiny.py"
            ),
        ),
    ]

    missing_bundle_files = [
        path for path in BUNDLE_FILES if not (PROJECT_ROOT / path).is_file()
    ]
    checks.append(
        PreflightCheck(
            label="deployment bundle files",
            passed=not missing_bundle_files,
            detail=f"missing: {', '.join(missing_bundle_files)}",
        )
    )

    source_files = list(PROJECT_ROOT.glob(CAPACITY_SOURCE_GLOB))
    checks.append(
        PreflightCheck(
            label="capacity-conversion source files",
            passed=bool(source_files),
            detail=f"no files match {CAPACITY_SOURCE_GLOB}",
        )
    )

    required_env_vars = [*CONNECT_ENV_VARS, *RUNTIME_ENV_VARS]
    if deployment_type is DeploymentType.REDEPLOY:
        required_env_vars.append("CONNECT_APP_ID")

    checks.extend(
        _environment_preflight_check(env_var, environment_sources)
        for env_var in required_env_vars
    )
    return checks


def print_preflight_checks(checks: list[PreflightCheck]) -> None:
    """Print prerequisite results without displaying configuration values."""
    print("\nPreflight checks:")
    for check in checks:
        if check.passed:
            status = "OK"
        elif check.required:
            status = check.failure_status
        else:
            status = "NOTICE"

        source = f" (source: {check.source})" if check.source is not None else ""
        print(f"  {status:<7} {check.label}{source}")
        if not check.passed and check.detail:
            print(f"          {check.detail}")


def build_deploy_command(
    deployment_type: DeploymentType,
    rsconnect_executable: str,
) -> list[str]:
    """Build the rsconnect argument list without invoking a shell."""
    command = [
        rsconnect_executable,
        "deploy",
        "shiny",
        "--title",
        APP_TITLE,
        "--entrypoint",
        "app:app",
        "--requirements-file",
        "requirements.txt",
        "--package-installer",
        "UV",
    ]

    if deployment_type is DeploymentType.NEW:
        command.append("--new")
    else:
        command.extend(["--app-id", os.environ["CONNECT_APP_ID"]])

    for env_var in RUNTIME_ENV_VARS:
        command.extend(["-E", env_var])

    command.extend(["--exclude=**", ".", *BUNDLE_FILES])
    command.extend(
        path.relative_to(PROJECT_ROOT).as_posix()
        for path in sorted(PROJECT_ROOT.glob(CAPACITY_SOURCE_GLOB))
    )
    return command


def confirm_deployment(deployment_type: DeploymentType) -> bool:
    """Show a secret-free summary and request final confirmation."""
    print("\nDeployment summary:")
    print(f"  Operation: {deployment_type.value}")
    print(f"  Title:     {APP_TITLE}")
    print(f"  Server:    {os.environ['CONNECT_SERVER']}")
    if deployment_type is DeploymentType.REDEPLOY:
        print(f"  App GUID:  {os.environ['CONNECT_APP_ID']}")

    try:
        choice = input("\nContinue with deployment? [y/N]: ").strip().casefold()
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    return choice in {"y", "yes"}


def run_command(command: list[str]) -> int:
    """Run a command from the project root and return its exit status."""
    try:
        completed = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
    except OSError as error:
        print(f"Unable to start {Path(command[0]).name}: {error}")
        return 1
    return completed.returncode


def main() -> int:
    """Validate configuration and deploy the Shiny app to Posit Connect."""
    environment_sources = load_deployment_environment()

    print("NHP Capacity Conversion deployment\n")
    deployment_type = choose_deployment_type()
    if deployment_type is None:
        print("Deployment cancelled.")
        return 0

    checks = collect_preflight_checks(deployment_type, environment_sources)
    print_preflight_checks(checks)
    if any(not check.passed and check.required for check in checks):
        print("\nDeployment cannot continue.")
        return 1

    if not confirm_deployment(deployment_type):
        print("Deployment cancelled.")
        return 0

    rsconnect_executable = shutil.which("rsconnect")
    if rsconnect_executable is None:  # Guarded by the preflight checks.
        print("rsconnect is no longer available.")
        return 1

    print("\nChecking the Posit Connect connection...")
    if (
        run_command(
            [
                rsconnect_executable,
                "details",
                "--server",
                os.environ["CONNECT_SERVER"],
            ]
        )
        != 0
    ):
        print("Connection check failed; deployment was not started.")
        return 1

    print("\nStarting deployment...")
    return run_command(build_deploy_command(deployment_type, rsconnect_executable))


if __name__ == "__main__":
    raise SystemExit(main())

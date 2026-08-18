"""Interactive deployment helper for the Shiny application."""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
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


class DeploymentType(StrEnum):
    """Supported Posit Connect deployment operations."""

    NEW = "new"
    REDEPLOY = "redeploy"


@dataclass(frozen=True)
class PreflightCheck:
    """One deployment prerequisite and its diagnostic result."""

    label: str
    passed: bool
    detail: str = ""
    required: bool = True


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


def collect_preflight_checks(deployment_type: DeploymentType) -> list[PreflightCheck]:
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
            detail="run this command with: uv run --locked --group dev nhp-deploy",
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
        PreflightCheck(
            label=env_var,
            passed=bool(os.getenv(env_var, "").strip()),
            detail="set it in .env or the current environment",
        )
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
            status = "MISSING"
        else:
            status = "NOTICE"

        print(f"  {status:<7} {check.label}")
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
    load_dotenv(dotenv_path=ENV_FILE, override=False)

    print("NHP Capacity Conversion deployment\n")
    deployment_type = choose_deployment_type()
    if deployment_type is None:
        print("Deployment cancelled.")
        return 0

    checks = collect_preflight_checks(deployment_type)
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

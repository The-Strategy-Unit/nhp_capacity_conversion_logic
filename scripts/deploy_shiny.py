"""Deploy the Shiny application to Posit Connect."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from http.client import HTTPMessage
from pathlib import Path
from time import sleep
from typing import IO
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from dotenv import dotenv_values, load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

CONNECT_ENV_VARS = (
    "CONNECT_SERVER",
    "CONNECT_API_KEY",
)
HTTPS_ENV_VARS = frozenset(
    {
        "AZ_STORAGE_EP",
        "AZ_TABLE_ENDPOINT",
        "CONNECT_SERVER",
        "FEEDBACK_FORM_URL",
    }
)
REQUIRED_RUNTIME_ENV_VARS = (
    "AZ_STORAGE_EP",
    "AZ_STORAGE_RESULTS",
    "AZ_TABLE_ENDPOINT",
    "CAPACITY_MODEL_VERSION",
    "TABLE_NAME",
    "FEEDBACK_FORM_URL",
)
RUNTIME_ENV_VARS = REQUIRED_RUNTIME_ENV_VARS
BUNDLE_FILES = (
    "app.py",
    "_brand.yml",
    "README.md",
    "pyproject.toml",
    "requirements.txt",
    "src/nhp/__init__.py",
    "www/app.css",
    "www/favicon.ico",
    "www/strategy-unit-nhs-logo.png",
)
CAPACITY_SOURCE_GLOB = "src/nhp/capacity_conversion/*.py"
DEPLOYMENT_ENV_VARS = (*CONNECT_ENV_VARS, *RUNTIME_ENV_VARS, "CONNECT_APP_ID")


class DeploymentType(StrEnum):
    """Supported Posit Connect deployment operations."""

    NEW = "new"
    REDEPLOY = "redeploy"


class DeploymentTarget(StrEnum):
    """Posit Connect environment targeted by a deployment."""

    DEV = "dev"
    PROD = "prod"


def _https_origin(value: str) -> tuple[str, int] | None:
    """Return a normalized HTTPS origin, rejecting ambiguous URLs."""
    try:
        parsed_url = urlparse(value)
        hostname = parsed_url.hostname
        port = parsed_url.port or 443
    except ValueError:
        return None

    if (
        parsed_url.scheme.casefold() != "https"
        or hostname is None
        or parsed_url.username is not None
        or parsed_url.password is not None
    ):
        return None
    return hostname.casefold(), port


class _SameOriginRedirectHandler(HTTPRedirectHandler):
    """Allow URL canonicalization without forwarding credentials off-origin."""

    def __init__(self, trusted_origin: tuple[str, int]) -> None:
        super().__init__()
        self.trusted_origin = trusted_origin

    def redirect_request(
        self,
        req: Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> Request | None:
        if _https_origin(newurl) != self.trusted_origin:
            return None
        return super().redirect_request(
            req,
            fp,
            code,
            msg,
            headers,
            newurl,
        )


APP_TITLES = {
    DeploymentTarget.DEV: "OpenPlan Capacity Conversion Model (development)",
    DeploymentTarget.PROD: "OpenPlan Capacity Conversion Model",
}
SMOKE_CHECK_ATTEMPTS = 3
SMOKE_CHECK_RETRY_SECONDS = 5


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


@dataclass(frozen=True)
class DeploymentOptions:
    """Command-line choices for interactive or automated deployment."""

    deployment_type: DeploymentType | None
    target: DeploymentTarget
    assume_yes: bool
    use_dotenv_file: bool


def parse_deployment_options(arguments: Sequence[str]) -> DeploymentOptions:
    """Parse deployment options without reading environment variables."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--deployment-type",
        choices=DeploymentType,
        type=DeploymentType,
        help="skip the interactive new/redeploy prompt",
    )
    parser.add_argument(
        "--target",
        choices=DeploymentTarget,
        default=DeploymentTarget.DEV,
        type=DeploymentTarget,
        help="deployment environment (default: dev)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="deploy without an interactive confirmation prompt",
    )
    parser.add_argument(
        "--no-dotenv",
        action="store_true",
        help="use only the current environment and do not read .env",
    )
    parsed = parser.parse_args(arguments)
    return DeploymentOptions(
        deployment_type=parsed.deployment_type,
        target=parsed.target,
        assume_yes=parsed.yes,
        use_dotenv_file=not parsed.no_dotenv,
    )


def load_deployment_environment(
    *,
    use_dotenv_file: bool = True,
) -> dict[str, EnvironmentSource]:
    """Load deployment values and record each effective value's source."""
    inherited_environment = set(os.environ)
    dotenv_environment = dotenv_values(dotenv_path=ENV_FILE) if use_dotenv_file else {}
    if use_dotenv_file:
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
    """Validate one deployment environment variable."""
    value = os.getenv(name, "").strip()
    source = environment_sources.get(name, EnvironmentSource.UNSET)
    if name in HTTPS_ENV_VARS and value and _https_origin(value) is None:
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

    required_env_vars = [*CONNECT_ENV_VARS, *REQUIRED_RUNTIME_ENV_VARS]
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
    target: DeploymentTarget = DeploymentTarget.DEV,
) -> list[str]:
    """Build the rsconnect argument list without invoking a shell."""
    command = [
        rsconnect_executable,
        "deploy",
        "shiny",
        "--title",
        APP_TITLES[target],
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


def confirm_deployment(
    deployment_type: DeploymentType,
    target: DeploymentTarget = DeploymentTarget.DEV,
) -> bool:
    """Show a secret-free summary and request final confirmation."""
    print("\nDeployment summary:")
    print(f"  Operation: {deployment_type.value}")
    print(f"  Target:    {target.value}")
    print(f"  Title:     {APP_TITLES[target]}")
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


def describe_deployment_target(
    rsconnect_executable: str,
    target: DeploymentTarget,
) -> str | None:
    """Validate the existing Connect content and return its HTTPS URL."""
    try:
        completed = subprocess.run(
            [
                rsconnect_executable,
                "content",
                "describe",
                "--guid",
                os.environ["CONNECT_APP_ID"],
            ],
            cwd=PROJECT_ROOT,
            check=False,
            stdout=subprocess.PIPE,
            text=True,
        )
    except OSError as error:
        print(f"Unable to start {Path(rsconnect_executable).name}: {error}")
        return None

    if completed.returncode != 0:
        return None

    try:
        details = json.loads(completed.stdout)
    except json.JSONDecodeError:
        print("Connect returned an invalid content description.")
        return None

    if not isinstance(details, dict) or (
        details.get("guid") != os.environ["CONNECT_APP_ID"]
        or details.get("app_mode") != "python-shiny"
        or details.get("title") != APP_TITLES[target]
    ):
        print(
            f"Connect content does not match the expected {target.value} application."
        )
        return None

    content_url = details.get("content_url")
    if not isinstance(content_url, str):
        print("Connect did not return an HTTPS content URL.")
        return None

    content_origin = _https_origin(content_url)
    connect_origin = _https_origin(os.environ["CONNECT_SERVER"])
    if content_origin is None or content_origin != connect_origin:
        print("Connect returned a content URL outside the configured server origin.")
        return None
    return content_url


def verify_deployed_application(content_url: str) -> bool:
    """Make an authenticated request to the deployed application."""
    trusted_origin = _https_origin(os.environ["CONNECT_SERVER"])
    if trusted_origin is None:  # Guarded by the preflight checks.
        return False

    application_request = Request(
        content_url,
        headers={"Authorization": f"Key {os.environ['CONNECT_API_KEY']}"},
    )
    opener = build_opener(_SameOriginRedirectHandler(trusted_origin))

    for attempt in range(1, SMOKE_CHECK_ATTEMPTS + 1):
        try:
            with opener.open(application_request, timeout=60) as response:
                response.read(1)
                status = response.status
        except HTTPError as error:
            retryable = error.code >= 500
        except (URLError, TimeoutError, OSError):
            retryable = True
        else:
            if status == 200:
                print("The deployed application passed its authenticated smoke check.")
                return True
            print("The deployed application did not return HTTP 200.")
            return False

        if not retryable or attempt == SMOKE_CHECK_ATTEMPTS:
            print(
                "The deployed application did not pass its authenticated smoke check."
            )
            return False
        sleep(SMOKE_CHECK_RETRY_SECONDS)

    return False  # Unreachable, but keeps the return contract explicit.


def main(arguments: Sequence[str] = ()) -> int:
    """Validate configuration and deploy the Shiny app to Posit Connect."""
    options = parse_deployment_options(arguments)
    environment_sources = load_deployment_environment(
        use_dotenv_file=options.use_dotenv_file
    )

    print("OpenPlan Capacity Conversion Model deployment\n")
    deployment_type = options.deployment_type or choose_deployment_type()
    if deployment_type is None:
        print("Deployment cancelled.")
        return 0

    checks = collect_preflight_checks(deployment_type, environment_sources)
    print_preflight_checks(checks)
    if any(not check.passed and check.required for check in checks):
        print("\nDeployment cannot continue.")
        return 1

    if not options.assume_yes and not confirm_deployment(
        deployment_type,
        options.target,
    ):
        print("Deployment cancelled.")
        return 0

    rsconnect_executable = shutil.which("rsconnect")
    if rsconnect_executable is None:  # Guarded by the preflight checks.
        print("rsconnect is no longer available.")
        return 1

    content_url: str | None = None
    if deployment_type is DeploymentType.REDEPLOY:
        print("\nChecking the Posit Connect deployment target...")
        content_url = describe_deployment_target(rsconnect_executable, options.target)
        if content_url is None:
            print("Connect pre-deployment check failed; deployment was not started.")
            return 1
    else:
        print("\nChecking the Posit Connect connection...")
        check_command = [
            rsconnect_executable,
            "details",
            "--server",
            os.environ["CONNECT_SERVER"],
        ]

        if run_command(check_command) != 0:
            print("Connect pre-deployment check failed; deployment was not started.")
            return 1

    print("\nStarting deployment...")
    deploy_status = run_command(
        build_deploy_command(
            deployment_type,
            rsconnect_executable,
            options.target,
        )
    )
    if deploy_status != 0 or content_url is None:
        return deploy_status

    print("\nChecking the deployed application...")
    if verify_deployed_application(content_url):
        return 0

    print(
        "The deployment may already be active. Follow the rollback procedure in "
        "README.md if the application is unhealthy."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

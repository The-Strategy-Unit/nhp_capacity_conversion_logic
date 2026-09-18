"""Synchronise Posit Connect viewers with eligible NHP model-run users."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from http.client import HTTPMessage
from typing import IO, cast
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, OpenerDirector, Request, build_opener
from uuid import UUID

from azure.data.tables import TableClient
from azure.identity import DefaultAzureCredential

MODEL_RUN_COLUMNS = ("user", "scenario", "run_stage", "app_version")
INDIVIDUAL_USERNAME = re.compile(r"^[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)+$")
CONNECT_PERMISSION_ROLES = frozenset({"viewer", "reviewer", "collaborator", "owner"})
CONNECT_PRINCIPAL_TYPES = frozenset({"user", "group"})
PROTECTED_PERMISSION_ROLES = frozenset({"collaborator", "owner"})
CONNECT_PAGE_SIZE = 500
CONNECT_TIMEOUT_SECONDS = 30


class AccessEnvironment(StrEnum):
    """Capacity application environment whose viewers are being managed."""

    DEV = "dev"
    PROD = "prod"


class SyncMode(StrEnum):
    """Whether a reconciliation previews or applies permission changes."""

    DRY_RUN = "dry-run"
    APPLY = "apply"


@dataclass(frozen=True)
class ConnectUser:
    """Connect identity needed to grant content viewership."""

    guid: str
    username: str
    locked: bool


@dataclass(frozen=True)
class ContentPermission:
    """One explicit Connect content permission."""

    id: str
    principal_guid: str
    principal_type: str
    role: str


@dataclass(frozen=True)
class SyncResult:
    """Aggregate reconciliation result safe to expose in job output."""

    eligible_users: int
    matched_users: int
    unmatched_users: int
    added_viewers: int
    removed_permissions: int
    dry_run: bool


@dataclass(frozen=True)
class SyncConfiguration:
    """Validated runtime configuration for one access-sync job."""

    table_endpoint: str
    table_name: str
    environment: AccessEnvironment
    qualifying_keywords: tuple[str, ...]
    connect_server: str
    connect_api_key: str
    target_content_guid: str
    mode: SyncMode


def _required_environment_variable(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _parse_access_sync_keywords(value: str) -> tuple[str, ...]:
    keywords = tuple(keyword.strip().casefold() for keyword in value.split(","))
    if any(not keyword for keyword in keywords):
        raise RuntimeError(
            "ACCESS_SYNC_KEYWORDS must be a comma-separated list of non-empty values"
        )
    return tuple(dict.fromkeys(keywords))


def load_sync_configuration() -> SyncConfiguration:
    """Load and validate access-sync configuration from the environment."""
    environment_value = _required_environment_variable("CAPACITY_MODEL_VERSION")
    mode_value = _required_environment_variable("ACCESS_SYNC_MODE")
    try:
        environment = AccessEnvironment(environment_value.casefold())
    except ValueError as error:
        raise RuntimeError("CAPACITY_MODEL_VERSION must be dev or prod") from error
    try:
        mode = SyncMode(mode_value.casefold())
    except ValueError as error:
        raise RuntimeError("ACCESS_SYNC_MODE must be dry-run or apply") from error

    table_endpoint = _required_environment_variable("AZ_TABLE_ENDPOINT")
    if _https_origin(table_endpoint) is None:
        raise RuntimeError("AZ_TABLE_ENDPOINT must be a valid HTTPS URL")
    connect_server = _required_environment_variable("CONNECT_SERVER")
    if _https_origin(connect_server) is None:
        raise RuntimeError("CONNECT_SERVER must be a valid HTTPS URL")
    target_content_guid = _required_environment_variable("CONNECT_APP_ID")
    try:
        UUID(target_content_guid)
    except ValueError as error:
        raise RuntimeError("CONNECT_APP_ID must be a valid UUID") from error

    return SyncConfiguration(
        table_endpoint=table_endpoint,
        table_name=_required_environment_variable("TABLE_NAME"),
        environment=environment,
        qualifying_keywords=_parse_access_sync_keywords(
            _required_environment_variable("ACCESS_SYNC_KEYWORDS")
        ),
        connect_server=connect_server,
        connect_api_key=_required_environment_variable("CONNECT_API_KEY"),
        target_content_guid=target_content_guid,
        mode=mode,
    )


def load_model_run_entities(table_endpoint: str, table_name: str) -> list[dict]:
    """Load the model-run fields used to establish Connect eligibility."""
    table_client = TableClient(
        endpoint=table_endpoint,
        table_name=table_name,
        credential=DefaultAzureCredential(),
    )
    return [
        dict(entity)
        for entity in table_client.list_entities(select=list(MODEL_RUN_COLUMNS))
    ]


def validate_model_run_entities(entities: list[dict]) -> None:
    """Reject an empty or incompatible model-run catalogue before ACL changes."""
    if not entities:
        raise ValueError("Model-run catalogue returned no entities")

    available_columns = set().union(*(entity.keys() for entity in entities))
    missing_columns = set(MODEL_RUN_COLUMNS).difference(available_columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Model-run catalogue is missing required columns: {missing}")

    has_usable_entity = any(
        isinstance(entity.get("user"), str)
        and isinstance(entity.get("app_version"), str)
        and any(
            isinstance(entity.get(field), str) for field in ("scenario", "run_stage")
        )
        for entity in entities
    )
    if not has_usable_entity:
        raise ValueError("Model-run catalogue has no usable entities")


def eligible_usernames(
    entities: Iterable[Mapping[str, object]],
    environment: AccessEnvironment,
    qualifying_keywords: tuple[str, ...],
) -> set[str]:
    """Return normalized individual usernames supported by qualifying runs."""
    eligible: set[str] = set()
    for entity in entities:
        username = entity.get("user")
        app_version = entity.get("app_version")
        if not isinstance(username, str) or not isinstance(app_version, str):
            continue

        username = username.strip()
        app_version = app_version.strip().casefold()
        if not INDIVIDUAL_USERNAME.fullmatch(username) or not app_version:
            continue
        if environment is AccessEnvironment.DEV and app_version != "dev":
            continue
        if environment is AccessEnvironment.PROD and app_version == "dev":
            continue

        has_qualifying_tag = any(
            tag in value.casefold()
            for field in ("scenario", "run_stage")
            if isinstance((value := entity.get(field)), str)
            for tag in qualifying_keywords
        )
        if has_qualifying_tag:
            eligible.add(username.casefold())
    return eligible


def _https_origin(value: str) -> tuple[str, int] | None:
    try:
        parsed = urlparse(value)
        hostname = parsed.hostname
        port = parsed.port or 443
    except ValueError:
        return None
    if (
        parsed.scheme.casefold() != "https"
        or hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or bool(parsed.query)
        or bool(parsed.fragment)
    ):
        return None
    return hostname.casefold(), port


class _SameOriginRedirectHandler(HTTPRedirectHandler):
    """Prevent the Connect API key from following an off-origin redirect."""

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
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class ConnectClient:
    """Small Connect API client limited to user and content-permission operations."""

    def __init__(
        self,
        server: str,
        api_key: str,
        *,
        opener: OpenerDirector | None = None,
    ) -> None:
        trusted_origin = _https_origin(server)
        if trusted_origin is None:
            raise ValueError("CONNECT_SERVER must be a valid HTTPS URL")
        self.server = server.rstrip("/")
        self.api_key = api_key
        self.opener = opener or build_opener(_SameOriginRedirectHandler(trusted_origin))

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: Mapping[str, object] | None = None,
    ) -> object | None:
        data = None if body is None else json.dumps(body).encode()
        request = Request(
            f"{self.server}/__api__{path}",
            data=data,
            headers={
                "Accept": "application/json",
                "Authorization": f"Key {self.api_key}",
                **({"Content-Type": "application/json"} if data is not None else {}),
            },
            method=method,
        )
        with self.opener.open(request, timeout=CONNECT_TIMEOUT_SECONDS) as response:
            response_body = response.read()
        if not response_body:
            return None
        return json.loads(response_body)

    def list_users(self) -> list[ConnectUser]:
        """List every local Connect user, following API pagination."""
        users: list[ConnectUser] = []
        page_number = 1
        total: int | None = None
        while total is None or len(users) < total:
            query = urlencode(
                {"page_number": page_number, "page_size": CONNECT_PAGE_SIZE}
            )
            payload = self._request(f"/v1/users?{query}")
            if not isinstance(payload, dict):
                raise TypeError("Connect users response must be an object")
            payload = cast(dict[str, object], payload)
            results = payload.get("results")
            response_total = payload.get("total")
            current_page = payload.get("current_page")
            if (
                not isinstance(results, list)
                or type(response_total) is not int
                or response_total < 0
                or current_page != page_number
            ):
                raise ValueError("Connect users response is missing results or total")
            if total is not None and response_total != total:
                raise ValueError("Connect users total changed during pagination")
            total = response_total
            users.extend(_connect_user(item) for item in results)
            if not results and len(users) < total:
                raise ValueError(
                    "Connect users pagination ended before total was reached"
                )
            page_number += 1
        return users

    def list_permissions(self, content_guid: str) -> list[ContentPermission]:
        """List explicit permissions for one Connect content item."""
        payload = self._request(f"/v1/content/{content_guid}/permissions")
        if not isinstance(payload, list):
            raise TypeError("Connect permissions response must be a list")
        return [_content_permission(item) for item in payload]

    def get_content_access_type(self, content_guid: str) -> str:
        """Return the target content's validated Connect access type."""
        payload = self._request(f"/v1/content/{content_guid}")
        if not isinstance(payload, dict):
            raise TypeError("Connect content response must be an object")
        payload = cast(dict[str, object], payload)
        returned_guid = payload.get("guid")
        access_type = payload.get("access_type")
        if returned_guid != content_guid or not isinstance(access_type, str):
            raise ValueError("Connect content response does not match the target")
        return access_type

    def add_viewer(self, content_guid: str, user_guid: str) -> None:
        """Grant one Connect user viewer access without sending email."""
        self._request(
            f"/v1/content/{content_guid}/permissions",
            method="POST",
            body={
                "principal_guid": user_guid,
                "principal_type": "user",
                "role": "viewer",
                "send_email": False,
            },
        )

    def delete_permission(self, content_guid: str, permission_id: str) -> None:
        """Delete one explicit permission from a Connect content item."""
        self._request(
            f"/v1/content/{content_guid}/permissions/{permission_id}",
            method="DELETE",
        )


def _connect_user(value: object) -> ConnectUser:
    if not isinstance(value, dict):
        raise TypeError("Connect user must be an object")
    value = cast(dict[str, object], value)
    guid = value.get("guid")
    username = value.get("username")
    locked = value.get("locked", False)
    if (
        not isinstance(guid, str)
        or not guid.strip()
        or not isinstance(username, str)
        or not username.strip()
    ):
        raise ValueError("Connect user is missing guid or username")
    if not isinstance(locked, bool):
        raise TypeError("Connect user locked value must be boolean")
    return ConnectUser(guid=guid, username=username, locked=locked)


def _content_permission(value: object) -> ContentPermission:
    if not isinstance(value, dict):
        raise TypeError("Connect permission must be an object")
    value = cast(dict[str, object], value)
    fields = {
        name: value.get(name)
        for name in ("id", "principal_guid", "principal_type", "role")
    }
    if not all(isinstance(field, str) and field for field in fields.values()):
        raise ValueError("Connect permission is missing required string fields")
    return ContentPermission(**cast(dict[str, str], fields))


def reconcile_permissions(
    client: ConnectClient,
    content_guid: str,
    eligible: set[str],
    *,
    dry_run: bool,
) -> SyncResult:
    """Make explicit Connect permissions match the eligible username set."""
    connect_users = client.list_users()
    users_by_name: dict[str, list[ConnectUser]] = {}
    for user in connect_users:
        users_by_name.setdefault(user.username.casefold(), []).append(user)

    user_guids = {user.guid for user in connect_users}
    if len(user_guids) != len(connect_users):
        raise ValueError("Connect user GUIDs must be unique")

    duplicate_names = eligible.intersection(
        name for name, users in users_by_name.items() if len(users) > 1
    )
    if duplicate_names:
        raise ValueError("Eligible Connect usernames must be unique")

    matched_users = {
        users_by_name[username][0].guid
        for username in eligible.intersection(users_by_name)
        if not users_by_name[username][0].locked
    }
    permissions = client.list_permissions(content_guid)
    unexpected_roles = {
        permission.role
        for permission in permissions
        if permission.role not in CONNECT_PERMISSION_ROLES
    }
    if unexpected_roles:
        raise ValueError("Connect returned an unsupported content-permission role")
    unexpected_principal_types = {
        permission.principal_type
        for permission in permissions
        if permission.principal_type not in CONNECT_PRINCIPAL_TYPES
    }
    if unexpected_principal_types:
        raise ValueError("Connect returned an unsupported permission principal type")
    permission_principals = [
        (permission.principal_type, permission.principal_guid)
        for permission in permissions
    ]
    if len(permission_principals) != len(set(permission_principals)):
        raise ValueError("Connect permission principals must be unique")
    unknown_permission_users = {
        permission.principal_guid
        for permission in permissions
        if permission.principal_type == "user"
        and permission.principal_guid not in user_guids
    }
    if unknown_permission_users:
        raise ValueError(
            "Connect users response does not cover current user permissions"
        )

    satisfied_guids = {
        permission.principal_guid
        for permission in permissions
        if permission.principal_type == "user"
        and permission.role in {"viewer", *PROTECTED_PERMISSION_ROLES}
    }
    viewers_to_add = sorted(matched_users.difference(satisfied_guids))
    permissions_to_remove = sorted(
        (
            permission
            for permission in permissions
            if permission.role not in PROTECTED_PERMISSION_ROLES
            and not (
                permission.principal_type == "user"
                and permission.principal_guid in matched_users
            )
        ),
        key=lambda permission: permission.id,
    )

    if not dry_run:
        for user_guid in viewers_to_add:
            client.add_viewer(content_guid, user_guid)
        for permission in permissions_to_remove:
            client.delete_permission(content_guid, permission.id)

    return SyncResult(
        eligible_users=len(eligible),
        matched_users=len(matched_users),
        unmatched_users=len(eligible) - len(matched_users),
        added_viewers=len(viewers_to_add),
        removed_permissions=len(permissions_to_remove),
        dry_run=dry_run,
    )


def run_access_sync(configuration: SyncConfiguration) -> SyncResult:
    """Load model runs and reconcile the configured Connect content ACL."""
    entities = load_model_run_entities(
        configuration.table_endpoint,
        configuration.table_name,
    )
    validate_model_run_entities(entities)
    eligible = eligible_usernames(
        entities,
        configuration.environment,
        configuration.qualifying_keywords,
    )
    client = ConnectClient(
        configuration.connect_server,
        configuration.connect_api_key,
    )
    if (
        configuration.mode is SyncMode.APPLY
        and client.get_content_access_type(configuration.target_content_guid) != "acl"
    ):
        raise ValueError("Target Connect content must use ACL access before apply mode")
    return reconcile_permissions(
        client,
        configuration.target_content_guid,
        eligible,
        dry_run=configuration.mode is SyncMode.DRY_RUN,
    )


def run_access_sync_from_environment() -> SyncResult:
    """Run access synchronization using environment configuration."""
    return run_access_sync(load_sync_configuration())

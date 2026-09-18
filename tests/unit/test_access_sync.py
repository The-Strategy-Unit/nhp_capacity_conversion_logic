import json
from unittest.mock import call
from urllib.request import Request

import pytest

from nhp.capacity_conversion.access_sync import (
    AccessEnvironment,
    ConnectClient,
    ConnectUser,
    ContentPermission,
    SyncConfiguration,
    SyncMode,
    _connect_user,
    _content_permission,
    _https_origin,
    _parse_access_sync_keywords,
    _SameOriginRedirectHandler,
    eligible_usernames,
    load_model_run_entities,
    load_sync_configuration,
    reconcile_permissions,
    run_access_sync,
    run_access_sync_from_environment,
    validate_model_run_entities,
)

CONTENT_GUID = "11111111-2222-4333-8444-555555555555"
KEYWORDS = ("test", "validation")


def _run(**overrides) -> dict:
    entity = {
        "user": "user.one",
        "scenario": "ordinary-scenario",
        "run_stage": "",
        "app_version": "dev",
    }
    entity.update(overrides)
    return entity


def test_eligible_usernames_uses_either_tag_field_and_normalizes_users():
    entities = [
        _run(user=" User.One ", scenario="scenario-TEST"),
        _run(user="user.two", run_stage="validation_report_ndg2"),
        _run(user="user.three", scenario="not-selected"),
        _run(user="some-team", scenario="test"),
        _run(user=None, scenario="test"),
        _run(app_version=None, scenario="test"),
        _run(app_version="", scenario="test"),
    ]

    result = eligible_usernames(entities, AccessEnvironment.DEV, KEYWORDS)

    assert result == {"user.one", "user.two"}


def test_eligible_usernames_matches_tags_anywhere_case_insensitively():
    entities = [
        _run(user="user.one", scenario="pretested"),
        _run(user="user.two", run_stage="revalidation"),
    ]

    assert eligible_usernames(entities, AccessEnvironment.DEV, KEYWORDS) == {
        "user.one",
        "user.two",
    }


def test_eligible_usernames_separates_dev_and_production_versions():
    entities = [
        _run(user="user.dev", scenario="test", app_version=" DEV "),
        _run(user="user.prod", scenario="test", app_version="v6.0"),
    ]

    assert eligible_usernames(entities, AccessEnvironment.DEV, KEYWORDS) == {"user.dev"}
    assert eligible_usernames(entities, AccessEnvironment.PROD, KEYWORDS) == {
        "user.prod"
    }


def test_load_sync_configuration(mocker):
    mocker.patch.dict(
        "os.environ",
        {
            "AZ_TABLE_ENDPOINT": "https://table.example.test",
            "TABLE_NAME": "runs",
            "CAPACITY_MODEL_VERSION": "DEV",
            "CONNECT_SERVER": "https://connect.example.test/",
            "CONNECT_API_KEY": "secret",
            "CONNECT_APP_ID": CONTENT_GUID,
            "ACCESS_SYNC_KEYWORDS": " TEST, validation,test ",
            "ACCESS_SYNC_MODE": "DRY-RUN",
        },
        clear=True,
    )

    assert load_sync_configuration() == SyncConfiguration(
        table_endpoint="https://table.example.test",
        table_name="runs",
        environment=AccessEnvironment.DEV,
        qualifying_keywords=KEYWORDS,
        connect_server="https://connect.example.test/",
        connect_api_key="secret",
        target_content_guid=CONTENT_GUID,
        mode=SyncMode.DRY_RUN,
    )


@pytest.mark.parametrize(
    ("environment", "mode", "message"),
    [
        ("test", "apply", "CAPACITY_MODEL_VERSION must be dev or prod"),
        ("dev", "preview", "ACCESS_SYNC_MODE must be dry-run or apply"),
    ],
)
def test_load_sync_configuration_rejects_invalid_enums(
    mocker, environment, mode, message
):
    mocker.patch.dict(
        "os.environ",
        {
            "CAPACITY_MODEL_VERSION": environment,
            "ACCESS_SYNC_MODE": mode,
        },
        clear=True,
    )

    with pytest.raises(RuntimeError, match=message):
        load_sync_configuration()


def test_load_sync_configuration_requires_every_value(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(
        RuntimeError,
        match="Missing required environment variable: CAPACITY_MODEL_VERSION",
    ):
        load_sync_configuration()


def test_parse_access_sync_keywords_rejects_empty_values():
    with pytest.raises(RuntimeError, match="comma-separated list of non-empty"):
        _parse_access_sync_keywords("test,,validation")


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("AZ_TABLE_ENDPOINT", "http://table.example.test", "valid HTTPS URL"),
        ("CONNECT_SERVER", "http://connect.example.test", "valid HTTPS URL"),
        ("CONNECT_APP_ID", "not-a-guid", "valid UUID"),
    ],
)
def test_load_sync_configuration_rejects_invalid_boundaries(
    mocker, name, value, message
):
    environment = {
        "AZ_TABLE_ENDPOINT": "https://table.example.test",
        "TABLE_NAME": "runs",
        "CAPACITY_MODEL_VERSION": "dev",
        "CONNECT_SERVER": "https://connect.example.test",
        "CONNECT_API_KEY": "secret",
        "CONNECT_APP_ID": CONTENT_GUID,
        "ACCESS_SYNC_KEYWORDS": "test,validation",
        "ACCESS_SYNC_MODE": "dry-run",
    }
    environment[name] = value
    mocker.patch.dict("os.environ", environment, clear=True)

    with pytest.raises(RuntimeError, match=message):
        load_sync_configuration()


def test_load_model_run_entities_uses_projection(mocker):
    credential = mocker.patch(
        "nhp.capacity_conversion.access_sync.DefaultAzureCredential"
    ).return_value
    table_client_class = mocker.patch("nhp.capacity_conversion.access_sync.TableClient")
    table_client = table_client_class.return_value
    table_client.list_entities.return_value = [{"user": "user.one"}]

    result = load_model_run_entities("https://table.example.test", "runs")

    assert result == [{"user": "user.one"}]
    table_client.list_entities.assert_called_once_with(
        select=["user", "scenario", "run_stage", "app_version"]
    )
    table_client_class.assert_called_once_with(
        endpoint="https://table.example.test",
        table_name="runs",
        credential=credential,
    )


@pytest.mark.parametrize(
    ("entities", "message"),
    [
        ([], "returned no entities"),
        (
            [{"user": "user.one", "scenario": "test", "app_version": "dev"}],
            "missing required columns: run_stage",
        ),
        (
            [
                {
                    "user": None,
                    "scenario": None,
                    "run_stage": None,
                    "app_version": None,
                }
            ],
            "no usable entities",
        ),
    ],
)
def test_validate_model_run_entities_rejects_unsafe_catalogues(entities, message):
    with pytest.raises(ValueError, match=message):
        validate_model_run_entities(entities)


def test_validate_model_run_entities_accepts_columns_across_sparse_entities():
    validate_model_run_entities(
        [
            {"user": "user.one", "scenario": "test", "app_version": "dev"},
            {"run_stage": "validation"},
        ]
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("https://connect.example.test", ("connect.example.test", 443)),
        ("https://CONNECT.example.test:8443", ("connect.example.test", 8443)),
        ("http://connect.example.test", None),
        ("https://user@connect.example.test", None),
        ("https://connect.example.test?redirect=other", None),
        ("https://connect.example.test#fragment", None),
        ("https://connect.example.test:not-a-port", None),
    ],
)
def test_https_origin(value, expected):
    assert _https_origin(value) == expected


def test_same_origin_redirect_handler_blocks_another_origin(mocker):
    handler = _SameOriginRedirectHandler(("connect.example.test", 443))

    assert (
        handler.redirect_request(
            Request("https://connect.example.test/start"),
            mocker.Mock(),
            302,
            "Found",
            mocker.Mock(),
            "https://other.example.test/end",
        )
        is None
    )


def test_same_origin_redirect_handler_allows_same_origin(mocker):
    handler = _SameOriginRedirectHandler(("connect.example.test", 443))

    redirected = handler.redirect_request(
        Request("https://connect.example.test/start"),
        mocker.Mock(),
        302,
        "Found",
        mocker.Mock(),
        "https://connect.example.test/end",
    )

    assert redirected is not None
    assert redirected.full_url == "https://connect.example.test/end"


class _Response:
    def __init__(self, payload=None):
        self.payload = b"" if payload is None else json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return self.payload


def _client(mocker, *responses):
    opener = mocker.Mock()
    opener.open.side_effect = [_Response(response) for response in responses]
    return ConnectClient(
        "https://connect.example.test/",
        "secret",
        opener=opener,
    ), opener


def test_connect_client_requires_https_server():
    with pytest.raises(ValueError, match="valid HTTPS URL"):
        ConnectClient("http://connect.example.test", "secret")


def test_connect_client_builds_default_opener(mocker):
    build_opener = mocker.patch("nhp.capacity_conversion.access_sync.build_opener")

    client = ConnectClient("https://connect.example.test", "secret")

    assert client.opener is build_opener.return_value


def test_connect_client_lists_paginated_users(mocker):
    client, opener = _client(
        mocker,
        {
            "current_page": 1,
            "total": 2,
            "results": [{"guid": "one-guid", "username": "user.one", "locked": False}],
        },
        {
            "current_page": 2,
            "total": 2,
            "results": [{"guid": "two-guid", "username": "user.two", "locked": True}],
        },
    )

    assert client.list_users() == [
        ConnectUser("one-guid", "user.one", False),
        ConnectUser("two-guid", "user.two", True),
    ]
    requests = [item.args[0] for item in opener.open.call_args_list]
    assert "page_number=1" in requests[0].full_url
    assert "page_number=2" in requests[1].full_url
    assert requests[0].headers["Authorization"] == "Key secret"


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {"current_page": 1, "results": "invalid", "total": 1},
        {"current_page": 1, "results": [], "total": "1"},
        {"current_page": 2, "results": [], "total": 0},
        {"current_page": 1, "results": [], "total": -1},
        {"current_page": 1, "results": [], "total": True},
    ],
)
def test_connect_client_rejects_invalid_user_response(mocker, payload):
    client, _ = _client(mocker, payload)

    with pytest.raises((TypeError, ValueError), match="users response"):
        client.list_users()


def test_connect_client_rejects_truncated_user_pagination(mocker):
    client, _ = _client(mocker, {"current_page": 1, "total": 1, "results": []})

    with pytest.raises(ValueError, match="pagination ended"):
        client.list_users()


def test_connect_client_rejects_changing_user_total(mocker):
    client, _ = _client(
        mocker,
        {
            "current_page": 1,
            "total": 2,
            "results": [{"guid": "one", "username": "user.one"}],
        },
        {
            "current_page": 2,
            "total": 3,
            "results": [{"guid": "two", "username": "user.two"}],
        },
    )

    with pytest.raises(ValueError, match="total changed"):
        client.list_users()


def test_connect_client_manages_permissions(mocker):
    permission_payload = [
        {
            "id": "11",
            "principal_guid": "user-guid",
            "principal_type": "user",
            "role": "viewer",
        }
    ]
    client, opener = _client(mocker, permission_payload, None, None)

    assert client.list_permissions("content-guid") == [
        ContentPermission("11", "user-guid", "user", "viewer")
    ]
    client.add_viewer("content-guid", "new-guid")
    client.delete_permission("content-guid", "11")

    add_request = opener.open.call_args_list[1].args[0]
    assert add_request.method == "POST"
    assert json.loads(add_request.data) == {
        "principal_guid": "new-guid",
        "principal_type": "user",
        "role": "viewer",
        "send_email": False,
    }
    assert add_request.headers["Content-type"] == "application/json"
    delete_request = opener.open.call_args_list[2].args[0]
    assert delete_request.method == "DELETE"
    assert delete_request.full_url.endswith("/permissions/11")


def test_connect_client_gets_content_access_type(mocker):
    client, opener = _client(
        mocker,
        {"guid": "content-guid", "access_type": "acl"},
    )

    assert client.get_content_access_type("content-guid") == "acl"
    assert opener.open.call_args.args[0].full_url.endswith(
        "/__api__/v1/content/content-guid"
    )


@pytest.mark.parametrize(
    "payload",
    [None, [], {"guid": "other-guid", "access_type": "acl"}],
)
def test_connect_client_rejects_invalid_content_response(mocker, payload):
    client, _ = _client(mocker, payload)

    with pytest.raises((TypeError, ValueError), match="content response"):
        client.get_content_access_type("content-guid")


def test_connect_client_rejects_invalid_permissions_response(mocker):
    client, _ = _client(mocker, {"not": "a list"})

    with pytest.raises(TypeError, match="permissions response"):
        client.list_permissions("content-guid")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("invalid", "must be an object"),
        ({"username": "user.one"}, "missing guid or username"),
        (
            {"guid": "guid", "username": "user.one", "locked": "false"},
            "locked value must be boolean",
        ),
    ],
)
def test_connect_user_validation(value, message):
    with pytest.raises((TypeError, ValueError), match=message):
        _connect_user(value)


@pytest.mark.parametrize(
    "value",
    ["invalid", {"id": "1"}],
)
def test_content_permission_validation(value):
    with pytest.raises((TypeError, ValueError), match="Connect permission"):
        _content_permission(value)


def test_reconcile_permissions_adds_then_removes_and_preserves_operational_roles(
    mocker,
):
    client = mocker.Mock(spec=ConnectClient)
    client.list_users.return_value = [
        ConnectUser("eligible-guid", "User.One", False),
        ConnectUser("locked-guid", "user.locked", True),
        ConnectUser("old-guid", "user.old", False),
        ConnectUser("reviewer-guid", "user.reviewer", False),
        ConnectUser("collaborator-guid", "user.collaborator", False),
        ConnectUser("owner-guid", "user.owner", False),
    ]
    client.list_permissions.return_value = [
        ContentPermission("1", "old-guid", "user", "viewer"),
        ContentPermission("2", "group-guid", "group", "viewer"),
        ContentPermission("3", "reviewer-guid", "user", "reviewer"),
        ContentPermission("4", "collaborator-guid", "user", "collaborator"),
        ContentPermission("5", "owner-guid", "user", "owner"),
    ]

    result = reconcile_permissions(
        client,
        "content-guid",
        {"user.one", "user.locked", "user.missing"},
        dry_run=False,
    )

    assert result.eligible_users == 3
    assert result.matched_users == 1
    assert result.unmatched_users == 2
    assert result.added_viewers == 1
    assert result.removed_permissions == 3
    assert result.dry_run is False
    client.add_viewer.assert_called_once_with("content-guid", "eligible-guid")
    client.delete_permission.assert_has_calls(
        [
            call("content-guid", "1"),
            call("content-guid", "2"),
            call("content-guid", "3"),
        ]
    )
    assert client.method_calls.index(
        call.add_viewer("content-guid", "eligible-guid")
    ) < client.method_calls.index(call.delete_permission("content-guid", "1"))


def test_reconcile_permissions_dry_run_is_idempotent(mocker):
    client = mocker.Mock(spec=ConnectClient)
    client.list_users.return_value = [ConnectUser("guid", "user.one", False)]
    client.list_permissions.return_value = [
        ContentPermission("1", "guid", "user", "viewer")
    ]

    result = reconcile_permissions(
        client,
        "content-guid",
        {"user.one"},
        dry_run=True,
    )

    assert result.added_viewers == 0
    assert result.removed_permissions == 0
    assert result.dry_run is True
    client.add_viewer.assert_not_called()
    client.delete_permission.assert_not_called()


def test_reconcile_permissions_downgrades_an_eligible_reviewer(mocker):
    client = mocker.Mock(spec=ConnectClient)
    client.list_users.return_value = [ConnectUser("guid", "user.one", False)]
    client.list_permissions.return_value = [
        ContentPermission("1", "guid", "user", "reviewer")
    ]

    result = reconcile_permissions(
        client,
        "content-guid",
        {"user.one"},
        dry_run=False,
    )

    assert result.added_viewers == 1
    assert result.removed_permissions == 0
    client.add_viewer.assert_called_once_with("content-guid", "guid")
    client.delete_permission.assert_not_called()


def test_reconcile_permissions_rejects_duplicate_eligible_usernames(mocker):
    client = mocker.Mock(spec=ConnectClient)
    client.list_users.return_value = [
        ConnectUser("one", "user.one", False),
        ConnectUser("two", "USER.ONE", False),
    ]

    with pytest.raises(ValueError, match="must be unique"):
        reconcile_permissions(client, "content-guid", {"user.one"}, dry_run=False)

    client.list_permissions.assert_not_called()


def test_reconcile_permissions_rejects_unknown_roles_before_mutating(mocker):
    client = mocker.Mock(spec=ConnectClient)
    client.list_users.return_value = [ConnectUser("guid", "user.one", False)]
    client.list_permissions.return_value = [
        ContentPermission("1", "guid", "user", "unexpected")
    ]

    with pytest.raises(ValueError, match="unsupported content-permission role"):
        reconcile_permissions(client, "content-guid", set(), dry_run=False)

    client.add_viewer.assert_not_called()
    client.delete_permission.assert_not_called()


def test_reconcile_permissions_rejects_unknown_principal_types(mocker):
    client = mocker.Mock(spec=ConnectClient)
    client.list_users.return_value = []
    client.list_permissions.return_value = [
        ContentPermission("1", "guid", "service", "viewer")
    ]

    with pytest.raises(ValueError, match="unsupported permission principal type"):
        reconcile_permissions(client, "content-guid", set(), dry_run=False)

    client.add_viewer.assert_not_called()
    client.delete_permission.assert_not_called()


def test_reconcile_permissions_rejects_duplicate_permission_principals(mocker):
    client = mocker.Mock(spec=ConnectClient)
    client.list_users.return_value = [ConnectUser("guid", "user.one", False)]
    client.list_permissions.return_value = [
        ContentPermission("1", "guid", "user", "viewer"),
        ContentPermission("2", "guid", "user", "reviewer"),
    ]

    with pytest.raises(ValueError, match="permission principals must be unique"):
        reconcile_permissions(client, "content-guid", {"user.one"}, dry_run=False)

    client.add_viewer.assert_not_called()
    client.delete_permission.assert_not_called()


def test_reconcile_permissions_rejects_incomplete_user_listing(mocker):
    client = mocker.Mock(spec=ConnectClient)
    client.list_users.return_value = []
    client.list_permissions.return_value = [
        ContentPermission("1", "unlisted-guid", "user", "viewer")
    ]

    with pytest.raises(ValueError, match="does not cover current user permissions"):
        reconcile_permissions(client, "content-guid", set(), dry_run=False)

    client.add_viewer.assert_not_called()
    client.delete_permission.assert_not_called()


def test_reconcile_permissions_rejects_duplicate_user_guids(mocker):
    client = mocker.Mock(spec=ConnectClient)
    client.list_users.return_value = [
        ConnectUser("guid", "user.one", False),
        ConnectUser("guid", "user.two", False),
    ]

    with pytest.raises(ValueError, match="GUIDs must be unique"):
        reconcile_permissions(client, "content-guid", set(), dry_run=False)

    client.list_permissions.assert_not_called()


def test_run_access_sync_wires_sources_to_reconciliation(mocker):
    configuration = SyncConfiguration(
        table_endpoint="https://table.example.test",
        table_name="runs",
        environment=AccessEnvironment.PROD,
        qualifying_keywords=KEYWORDS,
        connect_server="https://connect.example.test",
        connect_api_key="secret",
        target_content_guid="content-guid",
        mode=SyncMode.APPLY,
    )
    load_entities = mocker.patch(
        "nhp.capacity_conversion.access_sync.load_model_run_entities",
        return_value=[_run(app_version="v6", scenario="test", run_stage="")],
    )
    client_class = mocker.patch("nhp.capacity_conversion.access_sync.ConnectClient")
    client_class.return_value.get_content_access_type.return_value = "acl"
    expected = mocker.sentinel.result
    reconcile = mocker.patch(
        "nhp.capacity_conversion.access_sync.reconcile_permissions",
        return_value=expected,
    )

    assert run_access_sync(configuration) is expected
    load_entities.assert_called_once_with("https://table.example.test", "runs")
    client_class.assert_called_once_with("https://connect.example.test", "secret")
    client_class.return_value.get_content_access_type.assert_called_once_with(
        "content-guid"
    )
    reconcile.assert_called_once_with(
        client_class.return_value,
        "content-guid",
        {"user.one"},
        dry_run=False,
    )


def test_run_access_sync_rejects_non_acl_target_before_reconciliation(mocker):
    configuration = SyncConfiguration(
        table_endpoint="https://table.example.test",
        table_name="runs",
        environment=AccessEnvironment.PROD,
        qualifying_keywords=KEYWORDS,
        connect_server="https://connect.example.test",
        connect_api_key="secret",
        target_content_guid="content-guid",
        mode=SyncMode.APPLY,
    )
    mocker.patch(
        "nhp.capacity_conversion.access_sync.load_model_run_entities",
        return_value=[_run(app_version="v6", scenario="test")],
    )
    client_class = mocker.patch("nhp.capacity_conversion.access_sync.ConnectClient")
    client_class.return_value.get_content_access_type.return_value = "logged_in"
    reconcile = mocker.patch(
        "nhp.capacity_conversion.access_sync.reconcile_permissions"
    )

    with pytest.raises(ValueError, match="must use ACL access"):
        run_access_sync(configuration)

    reconcile.assert_not_called()


def test_run_access_sync_dry_run_allows_non_acl_target(mocker):
    configuration = SyncConfiguration(
        table_endpoint="https://table.example.test",
        table_name="runs",
        environment=AccessEnvironment.DEV,
        qualifying_keywords=KEYWORDS,
        connect_server="https://connect.example.test",
        connect_api_key="secret",
        target_content_guid="content-guid",
        mode=SyncMode.DRY_RUN,
    )
    mocker.patch(
        "nhp.capacity_conversion.access_sync.load_model_run_entities",
        return_value=[_run(scenario="test")],
    )
    client_class = mocker.patch("nhp.capacity_conversion.access_sync.ConnectClient")
    reconcile = mocker.patch(
        "nhp.capacity_conversion.access_sync.reconcile_permissions"
    )

    assert run_access_sync(configuration) is reconcile.return_value
    client_class.return_value.get_content_access_type.assert_not_called()


def test_run_access_sync_from_environment(mocker):
    configuration = mocker.patch(
        "nhp.capacity_conversion.access_sync.load_sync_configuration"
    ).return_value
    run = mocker.patch("nhp.capacity_conversion.access_sync.run_access_sync")

    assert run_access_sync_from_environment() is run.return_value
    run.assert_called_once_with(configuration)

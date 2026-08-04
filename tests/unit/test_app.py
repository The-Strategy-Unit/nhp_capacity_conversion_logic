from collections.abc import Callable
from typing import cast

import pytest
from shiny import App, Inputs, Outputs, Session

import app as app_module


def test_app_is_shiny_app() -> None:
    assert isinstance(app_module.app, App)


def test_server_renders_status_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rendered_messages: list[str] = []

    def capture_message(function: Callable[[], str]) -> Callable[[], str]:
        rendered_messages.append(function())
        return function

    monkeypatch.setattr(app_module.render, "text", capture_message)

    app_module.server(
        cast(Inputs, None),
        cast(Outputs, None),
        cast(Session, None),
    )

    assert rendered_messages == ["NHP Capacity Conversion is running."]

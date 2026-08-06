from playwright.sync_api import Page
from shiny.playwright import controller
from shiny.pytest import create_app_fixture
from shiny.run import ShinyAppProc

app = create_app_fixture("../../app.py")


def test_app_displays_running_status(
    page: Page,
    app: ShinyAppProc,
) -> None:
    page.goto(app.url)

    status = controller.OutputText(page, "message")
    status.expect_value("NHP Capacity Conversion is running.")

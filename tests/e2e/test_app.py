from playwright.sync_api import Page, expect
from shiny.pytest import create_app_fixture
from shiny.run import ShinyAppProc

app = create_app_fixture("../../app.py")


def test_app_displays_capacity_conversion_interface(
    page: Page,
    app: ShinyAppProc,
) -> None:
    page.goto(app.url)

    expect(
        page.get_by_role("heading", name="Capacity Conversion Estimates")
    ).to_be_visible()
    expect(page.locator("#estimates")).to_be_attached()
    expect(page.get_by_role("link", name="Download Estimates")).to_be_visible()

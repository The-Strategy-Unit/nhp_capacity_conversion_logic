import re

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
    feedback = page.get_by_role("button", name="Feedback")
    download = page.get_by_role("link", name="Download Estimates")

    expect(feedback).to_be_visible()
    expect(feedback).to_have_class(re.compile(r"\bbtn-sm\b"))
    expect(download).to_be_visible()
    expect(download).to_have_class(re.compile(r"\bbtn-sm\b"))

    page_count = len(page.context.pages)
    feedback.click()

    feedback_dialog = page.get_by_role("dialog")
    close = feedback_dialog.get_by_role("button", name="Close")
    expect(feedback_dialog).to_be_visible()
    expect(close).to_have_class(re.compile(r"\bbtn-primary\b"))
    expect(close).to_have_class(re.compile(r"\bbtn-sm\b"))
    assert len(page.context.pages) == page_count

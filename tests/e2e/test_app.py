import re

from playwright.sync_api import Page, expect
from shiny.playwright import controller
from shiny.pytest import create_app_fixture
from shiny.run import ShinyAppProc

app = create_app_fixture(
    "fixtures/app.py",
    env={
        "FEEDBACK_FORM_URL": "",
        "SHINY_TESTMODE": "1",
    },
)


def test_app_displays_capacity_conversion_interface(
    page: Page,
    app: ShinyAppProc,
) -> None:
    page.goto(app.url)

    expect(
        page.get_by_role("heading", name="Capacity Conversion Estimates")
    ).to_be_visible()
    estimates = controller.OutputDataFrame(page, "estimates")
    expect(estimates.loc).to_be_visible()
    feedback = controller.InputActionButton(page, "feedback")
    download = controller.DownloadButton(page, "download_estimates")

    expect(feedback.loc).to_be_visible()
    expect(feedback.loc).to_have_class(re.compile(r"\bbtn-sm\b"))
    expect(download.loc).to_be_visible()
    expect(download.loc).to_have_class(re.compile(r"\bbtn-sm\b"))
    expect(download.loc).to_have_attribute("href", re.compile(r".+"))

    page_count = len(page.context.pages)
    feedback.click()

    feedback_dialog = page.get_by_role("dialog")
    close = feedback_dialog.get_by_role("button", name="Close")
    expect(feedback_dialog).to_be_visible()
    expect(feedback_dialog).to_contain_text(
        "The feedback form is not currently available."
    )
    expect(close).to_have_class(re.compile(r"\bbtn-primary\b"))
    expect(close).to_have_class(re.compile(r"\bbtn-sm\b"))
    assert len(page.context.pages) == page_count

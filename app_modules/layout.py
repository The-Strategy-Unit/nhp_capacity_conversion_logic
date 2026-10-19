"""Static layout components for the capacity conversion application."""

from htmltools import Tag, TagChild
from shiny import ui


def app_header_ui(title: str) -> Tag:
    """Build the application brand header."""
    return ui.tags.header(
        ui.div(
            ui.span(title, class_="fs-4 fw-semibold"),
            ui.div(
                ui.img(
                    src="strategy-unit-nhs-logo.png",
                    alt="The Strategy Unit and NHS",
                    class_="brand-logo-image",
                ),
                class_="brand-logo-frame",
            ),
            class_=(
                "container d-flex flex-column flex-sm-row align-items-start "
                "align-items-sm-center justify-content-sm-between gap-2 py-3"
            ),
        ),
        class_="border-bottom bg-white",
    )


def page_heading_ui(documentation_url: str, feedback: TagChild) -> Tag:
    """Build the page title and application-level actions."""
    return ui.div(
        ui.h1("Capacity estimates", class_="mb-0"),
        ui.div(
            ui.a(
                "Documentation",
                href=documentation_url,
                target="_blank",
                rel="noopener noreferrer",
                class_="btn btn-primary btn-sm",
            ),
            feedback,
            class_="d-flex align-items-center gap-2",
        ),
        class_=(
            "d-flex flex-column flex-sm-row align-items-sm-center "
            "justify-content-between gap-3 mb-3"
        ),
    )

"""Feedback dialog Shiny module."""

from urllib.parse import urlparse

from shiny import Inputs, Outputs, Session, module, reactive, ui


@module.ui
def feedback_ui():
    """Build the feedback action button."""
    return ui.input_action_button(
        "show",
        "Feedback",
        class_="btn-primary btn-sm",
    )


@module.server
def feedback_server(
    input: Inputs,
    output: Outputs,
    session: Session,
    *,
    feedback_form_url: str | None,
) -> None:
    """Show the configured feedback form or its unavailable fallback."""

    @reactive.effect
    @reactive.event(input.show)
    def show_feedback_form() -> None:
        feedback_url = urlparse(feedback_form_url or "")
        if feedback_url.scheme != "https" or not feedback_url.netloc:
            ui.modal_show(
                ui.modal(
                    ui.p("The feedback form is not currently available."),
                    title="Feedback",
                    easy_close=True,
                    footer=ui.modal_button(
                        "Close",
                        class_="btn-primary btn-sm",
                    ),
                )
            )
            return

        ui.modal_show(
            ui.modal(
                ui.tags.iframe(
                    src=feedback_form_url,
                    title="Feedback form",
                    style="width: 100%; height: 70vh; border: 0;",
                ),
                title="Feedback",
                size="l",
                easy_close=True,
                footer=ui.modal_button(
                    "Close",
                    class_="btn-primary btn-sm",
                ),
            )
        )

from shiny import App, Inputs, Outputs, Session, render, ui

app_ui = ui.page_fluid(
    ui.output_text("message"),
    title="NHP Capacity Conversion",
    theme=ui.Theme.from_brand(__file__),
)


def server(input: Inputs, output: Outputs, session: Session) -> None:
    @render.text
    def message() -> str:
        return "NHP Capacity Conversion is running."


app = App(app_ui, server)

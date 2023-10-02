from shiny import Inputs, Outputs, Session, App, reactive, render, ui
import pandas as pd
from pathlib import Path
import shiny.experimental as x

dir = Path(__file__).parent
data_path = dir / "llm-data.csv"


@reactive.file_reader(data_path)
def df():
    df = pd.read_csv("llm-data.csv")
    return df


app_ui = ui.page_fluid(
    ui.layout_sidebar(
        ui.panel_sidebar(
            ui.input_text_area("notes", "Notes"),
            ui.input_selectize(
                "labels",
                "Labels",
                choices=["Not relevant", "Offensive", "Wordy"],
                multiple=True,
            ),
        ),
        ui.panel_main(
            ui.navset_tab_card(
                ui.nav(
                    "Tab 1",
                    ui.row(ui.output_ui("review_ui_output")),
                    ui.row(
                        x.ui.layout_column_wrap(
                            1 / 3,
                            ui.input_action_button("skip", "Skip"),
                            ui.input_action_button("accept", "Accept"),
                            ui.input_action_button("reject", "Reject"),
                        )
                    ),
                ),
                ui.nav("Tab 2", ui.output_data_frame("data_table")),
            )
        ),
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    current_row = reactive.Value(0)

    @output
    @render.data_frame
    def data_table():
        return df()

    @output
    @render.ui
    def review_ui_output():
        return review_ui(generate_prompt_dict(df().iloc[[current_row()]]))

    @reactive.Effect
    @reactive.event(input.skip)
    def _():
        get_next_item()

    @reactive.Effect
    @reactive.event(input.accept)
    def _():
        enter_item(
            current_row(), labels=input.labels(), notes=input.notes(), decision="Accept"
        )
        get_next_item()

    @reactive.Effect
    @reactive.event(input.reject)
    def _():
        enter_item(
            current_row(), labels=input.labels(), notes=input.notes(), decision="Reject"
        )
        get_next_item()

    def get_next_item():
        current_row.set(current_row.get() + 1)

    def enter_item(
        row_number: int,
        decision: str,
        labels: list = [],
        notes: str = " ",
        file_path: Path = data_path,
    ) -> None:
        to_write = {
            "notes": notes,
            "decition": decision,
            "labels": "|".join(labels),
        }
        write_to_csv(file_path, row_number, to_write)


app = App(app_ui, server)


def review_ui(prompt_dict):
    prompt = prompt_dict["prompt"]
    options = prompt_dict["options"]
    return x.ui.card(
        ui.h1(prompt),
        *[ui.h4(opt) for opt in options],
    )


def generate_prompt_dict(df):
    return {
        "prompt": df["prompt"].values[0],
        "options": [f"{opt}: {df[opt].values[0]}" for opt in ["A", "B", "C", "D", "E"]],
    }


def write_to_csv(csv_path: Path, row_num: int, col_values_dict: dict):
    df = pd.read_csv(csv_path)
    for key, value in col_values_dict.items():
        df.at[row_num, key] = value
    df.to_csv(csv_path, index=False)

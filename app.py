from typing import List
from shiny import Inputs, Outputs, Session, App, reactive, render, ui
import pandas as pd
from pathlib import Path
import shiny.experimental as x
from queries import load_data_to_sqlite, get_next_record, write_to_db
import sqlite3
import os


# Populate the database, in a real application this would be done by
# another process.
load_data_to_sqlite(Path(__file__).parent / "llm-data.csv")


def db_last_modified() -> float:
    return os.path.getmtime(Path(__file__).parent / "llm-data.db")


# Reactive poll uses a cheap function (in this case checking the db timestamp)
# to figure out whent to run the expensive function. Since this is defined outside of the
# server function it will be shared by all users.
@reactive.poll(db_last_modified, 0.5)
def df() -> pd.DataFrame:
    conn = sqlite3.connect("llm-data.db")
    out = pd.read_sql_query("SELECT * FROM llm_data", conn)
    conn.close()
    return out


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
                    "Review",
                    ui.row(
                        x.ui.layout_column_wrap(
                            1 / 3,
                            ui.input_action_button("skip", "Skip"),
                            ui.input_action_button("accept", "Accept"),
                            ui.input_action_button("reject", "Reject"),
                        )
                    ),
                    ui.row(ui.output_ui("review_ui_output")),
                ),
                ui.nav("Data", ui.output_data_frame("data_table")),
            )
        ),
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    conn = sqlite3.connect("llm-data.db")
    current_row = reactive.Value(get_next_record(conn=conn, current_id=None))

    @output
    @render.data_frame
    def data_table() -> pd.DataFrame:
        return df()

    @output
    @render.ui
    def review_ui_output() -> x.ui.card:
        return review_ui(current_row())

    @reactive.Effect
    @reactive.event(input.skip)
    def skip() -> None:
        enter_item(current_row(), decision="Skip")
        get_next_item()

    @reactive.Effect
    @reactive.event(input.accept)
    def accept() -> None:
        enter_item(
            current_row(), labels=input.labels(), notes=input.notes(), decision="Accept"
        )
        get_next_item()

    @reactive.Effect
    @reactive.event(input.reject)
    def reject() -> None:
        enter_item(
            current_row(), labels=input.labels(), notes=input.notes(), decision="Reject"
        )
        get_next_item()

    def get_next_item() -> None:
        current_row.set(
            get_next_record(conn=conn, current_id=current_row().at[0, "id"])
        )

    def enter_item(
        row_number: int,
        decision: str,
        labels: List[str] = [],
        notes: str = " ",
    ) -> None:
        to_write = {
            "notes": notes,
            "decision": decision,
            "labels": "|".join(labels),
        }
        write_to_db(current_row().at[0, "id"], to_write, conn)


app = App(app_ui, server)


def review_ui(prompt_df: pd.DataFrame) -> x.ui.card:
    prompt = prompt_df["prompt"].values[0]
    options = [
        f"{opt}: {prompt_df[opt].values[0]}" for opt in ["A", "B", "C", "D", "E"]
    ]
    return x.ui.card(
        ui.h3(prompt),
        *[ui.p(opt) for opt in options],
    )

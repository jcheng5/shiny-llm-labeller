import sqlite3
import pandas as pd
from pathlib import Path
from typing import Union
import os


def load_data_to_sqlite(data_path: Path, db_path: str = "llm-data.db") -> None:
    os.unlink(db_path)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE llm_data (
            id INTEGER PRIMARY KEY,
            prompt TEXT,
            A TEXT,
            B TEXT,
            C TEXT,
            D TEXT,
            E TEXT,
            notes TEXT,
            decision TEXT,
            labels TEXT,
            lock INTEGER
        )
        """
    )
    df = pd.read_csv(data_path)
    for index, row in df.iterrows():
        c.execute(
            """
            INSERT INTO llm_data (id, prompt, A, B, C, D, E, notes, decision, labels, lock)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', 0)
            """,
            (
                index,
                row["prompt"],
                row["A"],
                row["B"],
                row["C"],
                row["D"],
                row["E"],
                row["notes"],
            ),
        )
    conn.commit()
    conn.close()


def write_to_db(id: int, col_values_dict: dict, conn: sqlite3.Connection) -> None:
    c = conn.cursor()
    for key, value in col_values_dict.items():
        c.execute(f"UPDATE llm_data SET {key} = '{value}' WHERE id = {id}")
    conn.commit()


def get_next_record(
    conn: sqlite3.Connection, current_id: Union[str, None] = None
) -> pd.DataFrame:
    c = conn.cursor()

    if current_id is not None:
        write_to_db(current_id, {"lock": "0"}, conn=conn)
    columns = [
        "id",
        "prompt",
        "A",
        "B",
        "C",
        "D",
        "E",
        "notes",
        "decision",
        "labels",
        "lock",
    ]
    c.execute(
        f"SELECT {', '.join(columns)} FROM llm_data WHERE lock = 0 AND decision = '' LIMIT 1"
    )
    result = c.fetchone()
    out = pd.DataFrame(
        [result],
        columns=columns,
    )
    write_to_db(out.at[0, "id"], {"lock": "1"}, conn=conn)
    return out

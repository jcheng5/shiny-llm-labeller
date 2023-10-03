import sqlite3
import pandas as pd
from pathlib import Path
from typing import Union
import os


def load_data_to_sqlite(data_path: Path, db_path: str = "llm-data.db") -> None:
    """
    Load data from a CSV file into a SQLite database.

    Args:
        data_path: Path to the CSV file containing the data.
        db_path: Path to the SQLite database file.
    """
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
    """
    Write values to a row in the SQLite database.

    Args:
        id: The id of the row to write to.
        col_values_dict: A dictionary of column names and their new values.
        conn: The SQLite connection object.
    """
    c = conn.cursor()
    for key, value in col_values_dict.items():
        c.execute(f"UPDATE llm_data SET {key} = '{value}' WHERE id = {id}")
    conn.commit()


def get_next_record(
    conn: sqlite3.Connection, current_id: Union[str, None] = None
) -> pd.DataFrame:
    """
    Get the next record from the SQLite database. This functions
    first unlocks the currently selected record, then selects the next
    record that is both unlocked and has no `decision` field, and finally
    locks that row so that other users will not be able to access it.

    Args:
        conn: The SQLite connection object.
        current_id: The id of the current record, if any.

    Returns:
        A pandas DataFrame containing the next record.
    """
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

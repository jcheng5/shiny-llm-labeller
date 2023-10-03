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
            E TEXT
        )
        """
    )

    c.execute(
        """
        CREATE TABLE reviews (
            id INTEGER PRIMARY KEY,
            decision TEXT,
            notes TEXT,
            labels TEXT,
            reviewer TEXT
        )
        """
    )

    df = pd.read_csv(data_path)
    for index, row in df.iterrows():
        c.execute(
            """
            INSERT INTO llm_data (id, prompt, A, B, C, D, E)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                index,
                row["prompt"],
                row["A"],
                row["B"],
                row["C"],
                row["D"],
                row["E"],
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
    values = tuple(col_values_dict.values())
    placeholders = ",".join("?" * len(values))
    c.execute(
        f"INSERT INTO reviews ({','.join(col_values_dict.keys())}) VALUES ({placeholders})",
        values,
    )
    conn.commit()


def get_next_record(
    conn: sqlite3.Connection, current_id: Union[str, None] = None
) -> pd.DataFrame:
    """
    Get the next record from the SQLite database. This functions
    takes a random record from the table which hasn't yet been reviewed.
    This function allows the possibility of multiple reviews in the following scenario:
    - User A selects record
    - User B selects the same record before User A has completed their review
    - User A completes review
    - User B completes review

    The benefit of this approach is that we don't have to worry about locking records,
    or cleaning up the database when a session ends.

    Args:
        conn: The SQLite connection object.
        current_id: The id of the current record, if any.

    Returns:
        A pandas DataFrame containing the next record.
    """
    c = conn.cursor()

    columns = ["id", "prompt", "A", "B", "C", "D", "E"]
    c.execute(
        f"SELECT {', '.join(columns)} FROM llm_data WHERE id NOT IN (SELECT id FROM reviews) ORDER BY RANDOM() LIMIT 1"
    )
    result = c.fetchone()
    out = pd.DataFrame(
        [result],
        columns=columns,
    )
    return out

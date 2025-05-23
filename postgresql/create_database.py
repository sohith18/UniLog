import pandas as pd
from .db import get_connection

def create_table(table_name, csv_path, recreate=False):
    """
    Creates the main table and inserts data from a CSV file only if table is empty.
    Assumes the CSV columns match expected student table schema.
    """
    conn = get_connection()
    cur = conn.cursor()

    # Check if the table already exists
    if recreate:
        cur.execute(f""" DROP TABLE IF EXISTS {table_name}""")

    # Create the table
    cur.execute(f"""
        CREATE TABLE {table_name} (
            student_id TEXT,
            course_id TEXT,
            roll_no TEXT,
            email_id TEXT,
            grade TEXT,
            PRIMARY KEY (student_id, course_id)
        )
    """)
    conn.commit()

    # Check if table already has data
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cur.fetchone()[0]

    if row_count == 0:
        print(f"Table '{table_name}' is empty. Inserting data from CSV...")
        df = pd.read_csv(csv_path)

        for _, row in df.iterrows():
            cur.execute(f"""
                INSERT INTO {table_name} (student_id, course_id, roll_no, email_id, grade)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row['student_id'],
                row['course_id'],
                row['roll_no'],
                row['email'],
                row['grade']
            ))

        conn.commit()
        print(f"Data has been inserted successfully into '{table_name}'!")
    else:
        print(f"Table '{table_name}' already has data. Skipping CSV insertion.")

    cur.close()
    conn.close()

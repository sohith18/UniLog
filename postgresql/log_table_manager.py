from .db import get_connection
from .schema_utils import get_table_schema

def create_log_table(table_name,recreate=False):
    conn = get_connection()
    cur = conn.cursor()
    if recreate:
        cur.execute(f""" DROP TABLE IF EXISTS {table_name}_log""")

    schema = get_table_schema(table_name)
    log_table = f"{table_name}_log"

    # Create column definitions based on the schema
    col_defs = ", ".join([f"{col} TEXT" for col, _ in schema]) 

    extra_cols = "action TEXT, action_time INTEGER"
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {log_table} (
        {col_defs},
        {extra_cols}
    );
    """

    
    cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()

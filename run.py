#!/usr/bin/env python3
"""Execute SQL files against Snowflake in order."""
import sys
import os
import re
import snowflake.connector

SNOWFLAKE_CONFIG = {
    "account": "zozlgyg-dimagi_data_analytics",
    "user": "candidate_abd_user",
    "password": "vCWigj0z6lOUBNJh",
    "database": "DATA_ENGINEER_EXERCISE_ABD_DB",
    "role": "CANDIDATE_ABD_ROLE",
    "warehouse": "COMPUTE_WH",
}

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")


def run_sql_file(cursor, filepath):
    """Execute all statements in a SQL file."""
    with open(filepath, "r") as f:
        content = f.read()

    # Replace {DATA_DIR} placeholder with actual path
    content = content.replace("{DATA_DIR}", DATA_DIR)

    # Split on semicolons, preserving content inside quotes
    statements = split_sql(content)
    for stmt in statements:
        stmt = stmt.strip()
        if not stmt or (stmt.startswith("--") and "\n" not in stmt):
            continue
        preview = stmt.replace("\n", " ")[:100]
        print(f"  >> {preview}...")
        cursor.execute(stmt)
        try:
            results = cursor.fetchall()
            if results:
                # Print column headers if available
                if cursor.description:
                    headers = [col[0] for col in cursor.description]
                    print(f"    {headers}")
                for row in results:
                    print(f"    {row}")
        except snowflake.connector.errors.ProgrammingError:
            pass


def split_sql(content):
    """Split SQL on semicolons, ignoring semicolons inside single-quoted strings.
    Strips standalone comment lines so they don't get concatenated with statements."""
    # Remove standalone comment lines (lines that are only a comment)
    lines = content.split("\n")
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        cleaned_lines.append(line)
    content = "\n".join(cleaned_lines)

    statements = []
    current = []
    in_single_quote = False

    for char in content:
        if char == "'" and not in_single_quote:
            in_single_quote = True
            current.append(char)
        elif char == "'" and in_single_quote:
            in_single_quote = False
            current.append(char)
        elif char == ";" and not in_single_quote:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(char)

    # Don't forget trailing statement without semicolon
    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else None

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    try:
        if target:
            filepath = os.path.join(SQL_DIR, target)
            print(f"Running {target}...")
            run_sql_file(cursor, filepath)
        else:
            sql_files = sorted(f for f in os.listdir(SQL_DIR) if f.endswith(".sql"))
            for sql_file in sql_files:
                filepath = os.path.join(SQL_DIR, sql_file)
                print(f"\n{'='*60}")
                print(f"Running {sql_file}...")
                print(f"{'='*60}")
                run_sql_file(cursor, filepath)
    finally:
        cursor.close()
        conn.close()

    print("\nDone.")


if __name__ == "__main__":
    main()

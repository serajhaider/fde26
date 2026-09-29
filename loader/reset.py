import os
import argparse
import psycopg2
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
from config_loader import CONFIG


BRONZE_SCHEMA = "bronze"

DB_CONFIG = {
    "host": CONFIG['database']['analytics']['host'],
    "port": CONFIG['database']['analytics']['port'],
    "dbname": CONFIG['database']['analytics']['name'],
    "user": CONFIG['database']['analytics']['user'],
    "password": CONFIG['database']['analytics']['password'],
}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_bronze_tables(cursor):
    """
    Return all base tables from the bronze schema.
    """

    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """,
        (BRONZE_SCHEMA,),
    )

    return [row[0] for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display_tables(tables):
    print("\n" + "=" * 60)
    print(f"Tables in schema: {BRONZE_SCHEMA}")
    print("=" * 60)

    for index, table in enumerate(tables, start=1):
        print(f"  [{index}] {table}")

    print("=" * 60)


# ---------------------------------------------------------------------------
# Table selection
# ---------------------------------------------------------------------------

def select_tables(tables):
    """
    Ask the user to select one or more tables.

    Example:
        1,3,5
    """

    while True:
        selection = input(
            "\nSelect table(s) by number "
            "(e.g. 1,3,5): "
        ).strip()

        try:
            indexes = [
                int(value.strip())
                for value in selection.split(",")
            ]

            # Remove duplicates while preserving order
            indexes = list(dict.fromkeys(indexes))

            if not indexes:
                raise ValueError

            if any(
                index < 1 or index > len(tables)
                for index in indexes
            ):
                raise ValueError

            return [tables[index - 1] for index in indexes]

        except ValueError:
            print(
                "❌ Invalid selection. "
                "Please enter valid table numbers."
            )


# ---------------------------------------------------------------------------
# Operation selection
# ---------------------------------------------------------------------------

def select_operation():
    """
    Ask whether the user wants to truncate or drop.
    """

    print("\n" + "=" * 60)
    print("Select operation")
    print("=" * 60)
    print("  [1] TRUNCATE")
    print("      Remove all rows but keep the table.")
    print()
    print("  [2] DROP")
    print("      Permanently remove the table.")
    print("=" * 60)

    while True:
        choice = input("\nEnter choice [1/2]: ").strip()

        if choice == "1":
            return "truncate"

        if choice == "2":
            return "drop"

        print("❌ Invalid choice. Enter 1 or 2.")


# ---------------------------------------------------------------------------
# Confirmation
# ---------------------------------------------------------------------------

def confirm(tables, operation):
    print("\n" + "!" * 60)

    if operation == "truncate":
        print("⚠️  WARNING: The following tables will be TRUNCATED:")
    else:
        print("⚠️  WARNING: The following tables will be DROPPED:")

    print()

    for table in tables:
        print(f"  - {BRONZE_SCHEMA}.{table}")

    print("\n" + "!" * 60)

    if operation == "drop":
        print(
            "⚠️  DROP will permanently remove the table structure "
            "and all its data."
        )

    answer = input(
        "\nAre you sure you want to continue? [y/N]: "
    ).strip().lower()

    return answer == "y"


# ---------------------------------------------------------------------------
# Execute operation
# ---------------------------------------------------------------------------

def execute_operation(cursor, tables, operation):

    for table in tables:

        # Identifiers cannot be passed using %s parameters,
        # therefore they are safely quoted using psycopg2.sql.
        from psycopg2 import sql

        qualified_table = sql.Identifier(
            BRONZE_SCHEMA,
            table,
        )

        if operation == "truncate":

            query = sql.SQL(
                "TRUNCATE TABLE {} RESTART IDENTITY CASCADE;"
            ).format(qualified_table)

            print(
                f"🗑️  Truncating "
                f"{BRONZE_SCHEMA}.{table}"
            )

        elif operation == "drop":

            query = sql.SQL(
                "DROP TABLE {} CASCADE;"
            ).format(qualified_table)

            print(
                f"💥 Dropping "
                f"{BRONZE_SCHEMA}.{table}"
            )

        cursor.execute(query)


# ---------------------------------------------------------------------------
# --all
# ---------------------------------------------------------------------------

def reset_all():
    """
    Process every table in bronze.
    """

    connection = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        tables = get_bronze_tables(cursor)

        if not tables:
            print(
                f"\n⚠️  No tables found in "
                f"'{BRONZE_SCHEMA}' schema."
            )
            return

        display_tables(tables)

        operation = select_operation()

        if not confirm(tables, operation):
            print("\n❌ Operation cancelled.")
            return

        execute_operation(
            cursor,
            tables,
            operation,
        )

        connection.commit()

        print("\n" + "=" * 60)
        print(f"✅ Successfully completed: {operation.upper()}")
        print("=" * 60)

    except Exception as error:

        if connection:
            connection.rollback()

        print("\n❌ Operation failed.")
        print(f"Error: {error}")

        raise

    finally:

        if connection:
            connection.close()


# ---------------------------------------------------------------------------
# --only
# ---------------------------------------------------------------------------

def reset_only():
    """
    Allow the user to select specific bronze tables.
    """

    connection = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        tables = get_bronze_tables(cursor)

        if not tables:
            print(
                f"\n⚠️  No tables found in "
                f"'{BRONZE_SCHEMA}' schema."
            )
            return

        display_tables(tables)

        selected_tables = select_tables(tables)

        print("\nSelected tables:")

        for table in selected_tables:
            print(f"  - {BRONZE_SCHEMA}.{table}")

        operation = select_operation()

        if not confirm(selected_tables, operation):
            print("\n❌ Operation cancelled.")
            return

        execute_operation(
            cursor,
            selected_tables,
            operation,
        )

        connection.commit()

        print("\n" + "=" * 60)
        print(f"✅ Successfully completed: {operation.upper()}")
        print("=" * 60)

    except Exception as error:

        if connection:
            connection.rollback()

        print("\n❌ Operation failed.")
        print(f"Error: {error}")

        raise

    finally:

        if connection:
            connection.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Reset tables from the bronze schema."
        )
    )

    group = parser.add_mutually_exclusive_group(
        required=True
    )

    group.add_argument(
        "--all",
        action="store_true",
        help="Reset all tables in the bronze schema.",
    )

    group.add_argument(
        "--only",
        action="store_true",
        help=(
            "Select one or more tables from the "
            "bronze schema."
        ),
    )

    args = parser.parse_args()

    if args.all:
        reset_all()

    elif args.only:
        reset_only()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
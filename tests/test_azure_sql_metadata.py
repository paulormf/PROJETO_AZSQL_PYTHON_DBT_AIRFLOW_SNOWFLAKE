
from src.azure_sql.connection_sql import get_connection


def main():
    conn = get_connection()

    try:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                c.name AS column_name,
                ty.name AS data_type,
                c.is_nullable
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s
                ON s.schema_id = t.schema_id
            INNER JOIN sys.columns AS c
                ON c.object_id = t.object_id
            INNER JOIN sys.types AS ty
                ON ty.user_type_id = c.user_type_id
            WHERE s.name = 'dbo'
            ORDER BY t.name, c.column_id;
        """)

        rows = cursor.fetchall()

        current_table = None

        for row in rows:
            table_name = f"{row.schema_name}.{row.table_name}"

            if table_name != current_table:
                print(f"\n{table_name}")
                current_table = table_name

            print(
                f"  {row.column_name:<25} "
                f"{row.data_type:<15} "
                f"nullable={row.is_nullable}"
            )

        cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

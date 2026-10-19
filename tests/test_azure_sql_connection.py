from src.azure_sql.connection_sql import get_connection


def main():
    conn = get_connection()

    try:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT
                    DB_NAME() AS database_name,
                    SUSER_SNAME() AS login_name,
                    @@SERVERNAME AS server_name
            """)

            row = cursor.fetchone()

            print("Conexão com Azure SQL estabelecida!")
            print(f"Banco: {row.database_name}")
            print(f"Login: {row.login_name}")
            print(f"Servidor: {row.server_name}")

            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)

            tables = cursor.fetchall()

            print(f"\nTabelas encontradas: {len(tables)}")
            for table in tables:
                print(f"{table.TABLE_SCHEMA}.{table.TABLE_NAME}")

        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

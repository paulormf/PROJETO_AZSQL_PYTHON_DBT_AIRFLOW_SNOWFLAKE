
from src.snowflake.connection import get_connection


def main():
    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    CURRENT_ACCOUNT(),
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA(),
                    CURRENT_VERSION()
            """)

            row = cursor.fetchone()
            print("Conexão com Snowflake estabelecida!")
            print(f"Conta: {row[0]}")
            print(f"Usuário: {row[1]}")
            print(f"Role: {row[2]}")
            print(f"Warehouse: {row[3]}")
            print(f"Database: {row[4]}")
            print(f"Schema: {row[5]}")
            print(f"Versão: {row[6]}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

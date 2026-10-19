import os

import pyodbc
from dotenv import load_dotenv

load_dotenv()


def _odbc_value(value: str) -> str:
    """Escapa valores para uso seguro na connection string ODBC."""
    return "{" + value.replace("}", "}}") + "}"


def get_connection():
    """Abre uma conexão com o Azure SQL usando variáveis do .env."""

    required_vars = [
        "AZURE_SQL_SERVER",
        "AZURE_SQL_DATABASE",
        "AZURE_SQL_USER",
        "AZURE_SQL_PASSWORD",
    ]

    missing = [name for name in required_vars if not os.getenv(name)]
    if missing:
        raise ValueError(
            f"Variáveis ausentes na configuração: {', '.join(missing)}"
        )

    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={_odbc_value(os.environ['AZURE_SQL_SERVER'])};"
        f"DATABASE={_odbc_value(os.environ['AZURE_SQL_DATABASE'])};"
        f"UID={_odbc_value(os.environ['AZURE_SQL_USER'])};"
        f"PWD={_odbc_value(os.environ['AZURE_SQL_PASSWORD'])};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    return pyodbc.connect(connection_string, timeout=30)

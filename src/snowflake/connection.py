import os

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Abre uma conexão com o Snowflake usando variáveis de ambiente."""

    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]

    missing = [name for name in required_vars if not os.getenv(name)]
    if missing:
        raise ValueError(
            f"Variáveis ausentes na configuração: {', '.join(missing)}"
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR", "snowflake"),
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

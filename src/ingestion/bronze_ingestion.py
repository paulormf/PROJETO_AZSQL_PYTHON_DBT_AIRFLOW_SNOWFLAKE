
import argparse
import hashlib
import json
import re
import sys
import traceback
import uuid

from src.azure_sql.connection_sql import (
    get_connection as get_azure_connection,
)
from src.config.table_config import discover_table_config
from src.snowflake.connection import (
    get_connection as get_snowflake_connection,
)


PIPELINE_NAME = "AZURE_SQL_TO_SNOWFLAKE_BRONZE"
SOURCE_NAME = "AZURE_SQL"


def validate_identifier(value):
    """Valida identificadores SQL simples."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Identificador SQL inválido: {value}")

    return value


def serialize_row(row):
    """Serializa uma linha como JSON determinístico."""
    return json.dumps(
        row,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def get_row_hash(row):
    """Calcula o hash do conteúdo completo do registro."""
    payload = serialize_row(row)

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


def get_source_key(row, key_columns):
    """
    Calcula uma chave determinística.

    Usa a chave primária ou de negócio configurada.
    Se não houver chave definida, usa o hash do conteúdo.
    """
    if not key_columns:
        return get_row_hash(row)

    key_data = {
        column: row[column]
        for column in key_columns
    }

    payload = serialize_row(key_data)

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


def get_last_watermark(sf_cursor, source_table):
    """Obtém o último watermark registrado para a tabela."""
    sf_cursor.execute(
        """
        SELECT LAST_WATERMARK
        FROM OPS.PIPELINE_WATERMARKS
        WHERE PIPELINE_NAME = %s
          AND SOURCE_TABLE = %s
        """,
        (
            PIPELINE_NAME,
            source_table,
        ),
    )

    result = sf_cursor.fetchone()

    return result[0] if result else None


def extract_source_rows(table_config, last_watermark):
    """Extrai registros do Azure SQL conforme a configuração."""
    schema = validate_identifier(table_config["schema"])
    table = validate_identifier(table_config["table"])

    query = f"SELECT * FROM [{schema}].[{table}]"
    params = []

    if (
        table_config["load_type"] == "INCREMENTAL"
        and last_watermark is not None
    ):
        watermark_column = validate_identifier(
            table_config["watermark_column"]
        )

        query += f" WHERE [{watermark_column}] >= ?"
        params.append(last_watermark)

    source_connection = get_azure_connection()

    try:
        cursor = source_connection.cursor()
        cursor.execute(query, params)

        columns = [
            column[0]
            for column in cursor.description
        ]

        rows = [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]

        return rows

    finally:
        source_connection.close()


def create_bronze_table(sf_cursor, target_table):
    """Cria a tabela Bronze caso ainda não exista."""
    target_table = validate_identifier(target_table)

    sf_cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS BRONZE.{target_table} (
            SOURCE_TABLE VARCHAR NOT NULL,
            SOURCE_KEY VARCHAR NOT NULL,
            ROW_HASH VARCHAR NOT NULL,
            SOURCE_UPDATED_AT TIMESTAMP_NTZ,
            SOURCE_DATA VARIANT,
            INGESTED_AT TIMESTAMP_NTZ,
            RUN_ID VARCHAR
        )
        """
    )


def load_bronze(
    sf_cursor,
    target_table,
    source_table,
    rows,
    key_columns,
    run_id,
):
    """
    Carrega registros por staging e MERGE.

    Mantém versões distintas no Bronze quando o conteúdo
    de um registro muda. Versões idênticas não são inseridas
    novamente.
    """
    target_table = validate_identifier(target_table)
    staging_table = f"STG_{target_table}"

    if not rows:
        return 0

    sf_cursor.execute(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE {staging_table} (
            SOURCE_TABLE VARCHAR,
            SOURCE_KEY VARCHAR,
            ROW_HASH VARCHAR,
            SOURCE_UPDATED_AT TIMESTAMP_NTZ,
            SOURCE_DATA VARCHAR,
            RUN_ID VARCHAR
        )
        """
    )

    values = []

    for row in rows:
        row_hash = get_row_hash(row)
        source_key = get_source_key(row, key_columns)

        values.append(
            (
                source_table,
                source_key,
                row_hash,
                row.get("updated_at"),
                serialize_row(row),
                run_id,
            )
        )

    sf_cursor.executemany(
        f"""
        INSERT INTO {staging_table} (
            SOURCE_TABLE,
            SOURCE_KEY,
            ROW_HASH,
            SOURCE_UPDATED_AT,
            SOURCE_DATA,
            RUN_ID
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        values,
    )

    sf_cursor.execute(
        f"""
        MERGE INTO BRONZE.{target_table} AS target
        USING (
            SELECT
                SOURCE_TABLE,
                SOURCE_KEY,
                ROW_HASH,
                SOURCE_UPDATED_AT,
                PARSE_JSON(SOURCE_DATA) AS SOURCE_DATA,
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS INGESTED_AT,
                RUN_ID
            FROM {staging_table}
        ) AS source
           ON target.SOURCE_TABLE = source.SOURCE_TABLE
          AND target.SOURCE_KEY = source.SOURCE_KEY
          AND target.ROW_HASH = source.ROW_HASH
        WHEN NOT MATCHED THEN INSERT (
            SOURCE_TABLE,
            SOURCE_KEY,
            ROW_HASH,
            SOURCE_UPDATED_AT,
            SOURCE_DATA,
            INGESTED_AT,
            RUN_ID
        )
        VALUES (
            source.SOURCE_TABLE,
            source.SOURCE_KEY,
            source.ROW_HASH,
            source.SOURCE_UPDATED_AT,
            source.SOURCE_DATA,
            source.INGESTED_AT,
            source.RUN_ID
        )
        """
    )

    inserted_count = sf_cursor.rowcount

    sf_cursor.execute(
        f"DROP TABLE IF EXISTS {staging_table}"
    )

    return max(inserted_count, 0)


def update_watermark(
    sf_cursor,
    source_table,
    load_type,
    watermark,
):
    """Registra o watermark após uma carga bem-sucedida."""
    watermark_column = (
        "updated_at"
        if load_type == "INCREMENTAL"
        else None
    )

    sf_cursor.execute(
        """
        MERGE INTO OPS.PIPELINE_WATERMARKS AS target
        USING (
            SELECT
                %s AS PIPELINE_NAME,
                %s AS SOURCE_TABLE,
                %s AS WATERMARK_COLUMN,
                %s AS LAST_WATERMARK,
                %s AS LOAD_TYPE
        ) AS source
        ON target.PIPELINE_NAME = source.PIPELINE_NAME
           AND target.SOURCE_TABLE = source.SOURCE_TABLE

        WHEN MATCHED THEN UPDATE SET
            WATERMARK_COLUMN = source.WATERMARK_COLUMN,
            LAST_WATERMARK = COALESCE(
                source.LAST_WATERMARK,
                target.LAST_WATERMARK
            ),
            LAST_SUCCESS_AT = CURRENT_TIMESTAMP(),
            UPDATED_AT = CURRENT_TIMESTAMP(),
            LOAD_TYPE = source.LOAD_TYPE,
            STATUS = 'SUCCESS'

        WHEN NOT MATCHED THEN INSERT (
            PIPELINE_NAME,
            SOURCE_TABLE,
            WATERMARK_COLUMN,
            LAST_WATERMARK,
            LAST_SUCCESS_AT,
            UPDATED_AT,
            LOAD_TYPE,
            STATUS
        )
        VALUES (
            source.PIPELINE_NAME,
            source.SOURCE_TABLE,
            source.WATERMARK_COLUMN,
            source.LAST_WATERMARK,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            source.LOAD_TYPE,
            'SUCCESS'
        )
        """,
        (
            PIPELINE_NAME,
            source_table,
            watermark_column,
            watermark,
            load_type,
        ),
    )


def update_run_status(
    sf_cursor,
    run_id,
    status,
    source_count,
    inserted_count,
    error_message=None,
):
    """Atualiza o resultado da execução em OPS.PIPELINE_RUNS."""
    sf_cursor.execute(
        """
        UPDATE OPS.PIPELINE_RUNS
        SET STATUS = %s,
            FINISHED_AT = CURRENT_TIMESTAMP(),
            SOURCE_ROW_COUNT = %s,
            INSERTED_ROW_COUNT = %s,
            UPDATED_ROW_COUNT = 0,
            ERROR_MESSAGE = %s
        WHERE RUN_ID = %s
        """,
        (
            status,
            source_count,
            inserted_count,
            error_message[:4000] if error_message else None,
            run_id,
        ),
    )


def run_table(table_config):
    """Executa a ingestão de uma tabela."""
    table_name = validate_identifier(
        table_config["table"]
    )
    schema_name = validate_identifier(
        table_config["schema"]
    )

    source_table = f"{schema_name}.{table_name}"
    target_table = f"AZSQL_{table_name.upper()}"
    run_id = str(uuid.uuid4())

    sf_connection = get_snowflake_connection()
    sf_cursor = sf_connection.cursor()

    source_count = 0
    inserted_count = 0

    try:
        create_bronze_table(
            sf_cursor,
            target_table,
        )

        sf_cursor.execute(
            """
            INSERT INTO OPS.PIPELINE_RUNS (
                RUN_ID,
                PIPELINE_NAME,
                SOURCE_NAME,
                SOURCE_TABLE,
                LOAD_TYPE,
                STATUS,
                STARTED_AT
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                CURRENT_TIMESTAMP()
            )
            """,
            (
                run_id,
                PIPELINE_NAME,
                SOURCE_NAME,
                source_table,
                table_config["load_type"],
                "RUNNING",
            ),
        )

        sf_connection.commit()

        previous_watermark = None

        if table_config["load_type"] == "INCREMENTAL":
            previous_watermark = get_last_watermark(
                sf_cursor,
                source_table,
            )

        rows = extract_source_rows(
            table_config,
            previous_watermark,
        )

        source_count = len(rows)

        print(f"\nTabela: {source_table}")
        print(f"Tipo de carga: {table_config['load_type']}")
        print(f"Registros extraídos: {source_count}")

        inserted_count = load_bronze(
            sf_cursor,
            target_table,
            source_table,
            rows,
            table_config["key_columns"],
            run_id,
        )

        new_watermark = None

        if (
            table_config["load_type"] == "INCREMENTAL"
            and rows
        ):
            timestamps = [
                row["updated_at"]
                for row in rows
                if row.get("updated_at") is not None
            ]

            if timestamps:
                new_watermark = max(timestamps)

        update_watermark(
            sf_cursor,
            source_table,
            table_config["load_type"],
            new_watermark,
        )

        update_run_status(
            sf_cursor,
            run_id,
            "SUCCESS",
            source_count,
            inserted_count,
        )

        sf_connection.commit()

        print(
            f"Novas versões inseridas no Bronze: "
            f"{inserted_count}"
        )
        print("Status: SUCCESS")

    except Exception as exc:
        sf_connection.rollback()

        try:
            update_run_status(
                sf_cursor,
                run_id,
                "FAILED",
                source_count,
                inserted_count,
                str(exc),
            )
            sf_connection.commit()

        except Exception:
            sf_connection.rollback()

        raise

    finally:
        sf_cursor.close()
        sf_connection.close()


def main():
    parser = argparse.ArgumentParser(
        description="Ingestão Azure SQL para Snowflake Bronze"
    )

    execution = parser.add_mutually_exclusive_group(
        required=True
    )

    execution.add_argument(
        "--table",
        help="Executa uma tabela específica, por exemplo: brands",
    )

    execution.add_argument(
        "--all",
        action="store_true",
        help="Executa todas as tabelas configuradas",
    )

    args = parser.parse_args()
    tables = discover_table_config()

    if args.table:
        selected_tables = [
            table
            for table in tables
            if table["table"].lower() == args.table.lower()
        ]

        if not selected_tables:
            raise ValueError(
                f"Tabela não encontrada na configuração: {args.table}"
            )
    else:
        selected_tables = tables

    successful = []
    failed = []

    print(
        f"\nTotal de tabelas selecionadas: "
        f"{len(selected_tables)}"
    )

    for table_config in selected_tables:
        table_name = (
            f'{table_config["schema"]}.'
            f'{table_config["table"]}'
        )

        print(f"\n{'=' * 70}")
        print(f"Iniciando: {table_name}")
        print("=" * 70)

        try:
            run_table(table_config)
            successful.append(table_name)

        except Exception as exc:
            failed.append((table_name, str(exc)))

            print(f"\nFALHA na tabela: {table_name}")
            print(f"Erro: {exc}")
            traceback.print_exc()

    print(f"\n{'=' * 70}")
    print("RESUMO DA INGESTÃO BRONZE")
    print("=" * 70)
    print(f"Tabelas selecionadas: {len(selected_tables)}")
    print(f"Tabelas concluídas: {len(successful)}")
    print(f"Tabelas com falha: {len(failed)}")

    if successful:
        print("\nCONCLUÍDAS:")

        for table_name in successful:
            print(f"  OK - {table_name}")

    if failed:
        print("\nCOM FALHA:")

        for table_name, error in failed:
            print(f"  ERRO - {table_name}: {error}")

        sys.exit(1)

    print("\nTodas as tabelas foram processadas com sucesso.")


if __name__ == "__main__":
    main()

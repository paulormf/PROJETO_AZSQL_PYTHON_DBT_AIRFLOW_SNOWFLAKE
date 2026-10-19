from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

# Adiciona a raiz do projeto ao caminho de importação do Python.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.config.table_config import discover_table_config


DBT_DIR = ROOT_DIR / "dbt"
SILVER_DIR = DBT_DIR / "models" / "silver"
TESTS_DIR = DBT_DIR / "tests"


def yaml_string(value: str) -> str:
    """Produz uma string entre aspas duplas compatível com YAML."""
    return json.dumps(value, ensure_ascii=False)


def safe_identifier(value: str) -> str:
    """Valida identificadores usados em nomes de arquivos e objetos."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Identificador SQL inesperado: {value!r}")
    return value


def source_expression(column: dict[str, Any]) -> str:
    """Gera a conversão de um campo JSON para seu tipo SQL de destino."""

    name = safe_identifier(column["name"])
    data_type = column["data_type"].lower()
    precision = int(column.get("precision") or 0)
    scale = int(column.get("scale") or 0)

    # A sintaxe com aspas preserva nomes de propriedades JSON.
    raw = f'SOURCE_DATA:"{name}"::VARCHAR'

    integer_types = {"tinyint", "smallint", "int", "bigint"}
    decimal_types = {"decimal", "numeric"}
    string_types = {
        "char",
        "nchar",
        "varchar",
        "nvarchar",
        "text",
        "ntext",
        "sysname",
        "uniqueidentifier",
        "xml",
    }

    if data_type in integer_types:
        return f"TRY_TO_NUMBER({raw}, {precision or 38}, 0)"

    if data_type in decimal_types:
        return (
            f"TRY_TO_DECIMAL("
            f"{raw}, {precision or 38}, {scale}"
            f")"
        )

    if data_type in {"money", "smallmoney"}:
        return f"TRY_TO_DECIMAL({raw}, {precision or 19}, 4)"

    if data_type in {"float", "real"}:
        return f"TRY_TO_DOUBLE({raw})"

    if data_type == "bit":
        return f"TRY_TO_BOOLEAN({raw})"

    if data_type == "date":
        return f"TRY_TO_DATE({raw})"

    if data_type in {"datetime", "datetime2", "smalldatetime"}:
        return f"TRY_TO_TIMESTAMP_NTZ({raw})"

    if data_type == "datetimeoffset":
        return f"TRY_TO_TIMESTAMP_TZ({raw})"

    if data_type == "time":
        return f"TRY_TO_TIME({raw})"

    if data_type in string_types:
        return f"NULLIF(TRIM({raw}), '')"

    # SQL Server timestamp/rowversion é binário, não data/hora.
    # Tipos binários e tipos especiais ficam como texto nesta primeira
    # versão, evitando conversões temporais incorretas.
    return f"NULLIF(TRIM({raw}), '')"


def generate_model_sql(table: dict[str, Any]) -> str:
    table_name = safe_identifier(table["table"])
    model_name = f"silver_{table_name}"

    columns_sql = []

    for column in table["column_metadata"]:
        column_name = safe_identifier(column["name"])
        expression = source_expression(column)

        columns_sql.append(
            f"        {expression} AS {column_name}"
        )

    columns_sql.extend(
        [
            "        SOURCE_KEY AS source_key",
            "        ROW_HASH AS row_hash",
            "        SOURCE_UPDATED_AT AS source_updated_at",
            "        INGESTED_AT AS ingested_at",
            "        RUN_ID AS run_id",
        ]
    )

    typed_columns = ",\n".join(columns_sql)

    # Usa a chave lógica configurada. Se não houver uma chave conhecida,
    # SOURCE_KEY é usado para eliminar apenas repetições da mesma chave
    # técnica, sem inventar uma chave de negócio.
    key_columns = table["key_columns"]

    if key_columns:
        partition_by = ", ".join(
            safe_identifier(column) for column in key_columns
        )
    else:
        partition_by = "source_key"

    return f"""WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{{{ source('bronze', '{table_name}') }}}}

),

typed_data AS (

    SELECT
{typed_columns}
    FROM source_data

),

deduplicated AS (

    SELECT *
    FROM typed_data

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {partition_by}
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
"""


def generate_sources_yaml(tables: list[dict[str, Any]]) -> str:
    lines = [
        "version: 2",
        "",
        "sources:",
        "  - name: bronze",
        "    description: Dados brutos ingeridos do Azure SQL para o Snowflake.",
        "    database: COMMERCE_DWH_DEV",
        "    schema: BRONZE",
        "",
        "    tables:",
    ]

    for table in tables:
        name = safe_identifier(table["table"])
        lines.extend(
            [
                f"      - name: {name}",
                f"        identifier: AZSQL_{name.upper()}",
                (
                    "        description: "
                    + yaml_string(
                        f"Tabela bruta {name} originada no Azure SQL."
                    )
                ),
            ]
        )

    return "\n".join(lines) + "\n"


def generate_models_yaml(tables: list[dict[str, Any]]) -> str:
    lines = [
        "version: 2",
        "",
        "models:",
    ]

    for table in tables:
        table_name = safe_identifier(table["table"])
        model_name = f"silver_{table_name}"
        key_columns = set(table["key_columns"])

        lines.extend(
            [
                f"  - name: {model_name}",
                (
                    "    description: "
                    + yaml_string(
                        f"Dados tipados e deduplicados de {table_name}."
                    )
                ),
                "    columns:",
            ]
        )

        documented_columns: set[str] = set()

        for column in table["column_metadata"]:
            column_name = safe_identifier(column["name"])
            nullable = bool(column["nullable"])

            tests = []

            # Respeita a nulabilidade declarada pela origem e a chave
            # lógica escolhida para o modelo.
            if not nullable or column_name in key_columns:
                tests.append("not_null")

            if (
                table["key_type"] == "PRIMARY_KEY"
                and len(table["key_columns"]) == 1
                and column_name == table["key_columns"][0]
            ):
                tests.append("unique")

            lines.extend(
                [
                    f"      - name: {column_name}",
                    (
                        "        description: "
                        + yaml_string(
                            f"Coluna {column_name} da tabela de origem."
                        )
                    ),
                ]
            )

            if tests:
                lines.append("        data_tests:")
                for test in tests:
                    lines.append(f"          - {test}")

            documented_columns.add(column_name)

        # Metadados técnicos comuns a todos os modelos.
        for column_name, description in [
            ("source_key", "Identificador técnico do registro de origem."),
            ("row_hash", "Hash do conteúdo original na Bronze."),
            ("source_updated_at", "Data de atualização registrada na origem."),
            ("ingested_at", "Data e hora da ingestão na Bronze."),
            ("run_id", "Identificador da execução de ingestão."),
        ]:
            if column_name in documented_columns:
                continue

            lines.extend(
                [
                    f"      - name: {column_name}",
                    "        description: " + yaml_string(description),
                ]
            )

            if column_name in {"source_key", "row_hash"}:
                lines.extend(
                    [
                        "        data_tests:",
                        "          - not_null",
                    ]
                )

    return "\n".join(lines) + "\n"


def generate_composite_key_test(table: dict[str, Any]) -> str | None:
    table_name = safe_identifier(table["table"])
    key_columns = table["key_columns"]

    if len(key_columns) < 2:
        return None

    model_name = f"silver_{table_name}"
    safe_keys = [safe_identifier(column) for column in key_columns]
    columns_sql = ", ".join(safe_keys)

    return f"""SELECT
    {columns_sql},
    COUNT(*) AS duplicate_count
FROM {{{{ ref('{model_name}') }}}}
GROUP BY {columns_sql}
HAVING COUNT(*) > 1
"""


def build_outputs(tables: list[dict[str, Any]]) -> dict[Path, str]:
    outputs: dict[Path, str] = {}

    outputs[SILVER_DIR / "_sources.yml"] = generate_sources_yaml(tables)
    outputs[SILVER_DIR / "_silver_models.yml"] = generate_models_yaml(tables)

    for table in tables:
        table_name = safe_identifier(table["table"])
        model_path = SILVER_DIR / f"silver_{table_name}.sql"
        outputs[model_path] = generate_model_sql(table)

        composite_test = generate_composite_key_test(table)

        if composite_test:
            test_path = TESTS_DIR / f"unique_key_silver_{table_name}.sql"
            outputs[test_path] = composite_test

    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera modelos dbt Silver a partir dos metadados do Azure SQL."
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Grava os arquivos gerados. Sem esta opção, apenas lista os arquivos.",
    )
    args = parser.parse_args()

    tables = discover_table_config()
    outputs = build_outputs(tables)

    print(f"Tabelas descobertas: {len(tables)}")
    print(f"Arquivos previstos: {len(outputs)}")
    print(f"Modo: {'GRAVAÇÃO' if args.write else 'SIMULAÇÃO'}")
    print()

    for path, content in sorted(outputs.items(), key=lambda item: str(item[0])):
        relative_path = path.relative_to(ROOT_DIR)
        print(f"{'Gerando' if args.write else 'Previsto'}: {relative_path}")

        if args.write:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    if args.write:
        print("\nGeração concluída.")
    else:
        print("\nNenhum arquivo foi alterado. Use --write para gravar.")


if __name__ == "__main__":
    main()

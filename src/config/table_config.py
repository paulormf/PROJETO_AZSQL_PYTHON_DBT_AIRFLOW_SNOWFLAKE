
from src.azure_sql.connection_sql import get_connection


# Tabelas internas ou de controle que não devem ser ingeridas como dados de negócio.
EXCLUDED_TABLES = {
    "pipeline_watermark",
    "systranschemas",
}

# Chaves candidatas para tabelas sem chave primária declarada.
BUSINESS_KEYS = {
    "customer_campaigns": [
        "customer_id",
        "campaign_id",
        "interaction_date",
    ],
    "inventory_balance": [
        "product_id",
        "warehouse_id",
    ],
}


def discover_table_config():
    """
    Descobre metadados das tabelas dbo no SQL Server.

    Retorna:
    - schema e nome da tabela;
    - nomes das colunas;
    - tipos SQL, precisão, escala, tamanho e nulabilidade;
    - chave primária declarada ou chave de negócio candidata;
    - estratégia FULL ou INCREMENTAL;
    - coluna de watermark.
    """

    query = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            c.name AS column_name,
            c.column_id,
            ty.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            CASE
                WHEN pk.key_ordinal IS NOT NULL THEN 1
                ELSE 0
            END AS is_primary_key,
            pk.key_ordinal,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM sys.columns AS uc
                    WHERE uc.object_id = t.object_id
                      AND uc.name = 'updated_at'
                ) THEN 1
                ELSE 0
            END AS has_updated_at
        FROM sys.tables AS t
        INNER JOIN sys.schemas AS s
            ON s.schema_id = t.schema_id
        INNER JOIN sys.columns AS c
            ON c.object_id = t.object_id
        INNER JOIN sys.types AS ty
            ON ty.user_type_id = c.user_type_id
        LEFT JOIN (
            SELECT
                ic.object_id,
                ic.column_id,
                ic.key_ordinal
            FROM sys.indexes AS i
            INNER JOIN sys.index_columns AS ic
                ON ic.object_id = i.object_id
               AND ic.index_id = i.index_id
            WHERE i.is_primary_key = 1
        ) AS pk
            ON pk.object_id = c.object_id
           AND pk.column_id = c.column_id
        WHERE s.name = 'dbo'
          AND t.is_ms_shipped = 0
        ORDER BY
            t.name,
            c.column_id;
    """

    connection = get_connection()

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
    finally:
        connection.close()

    tables = {}

    for row in rows:
        table_name = row.table_name

        if table_name.lower() in EXCLUDED_TABLES:
            continue

        table_id = (row.schema_name, table_name)

        if table_id not in tables:
            tables[table_id] = {
                "schema": row.schema_name,
                "table": table_name,
                # Mantém a lista de nomes para compatibilidade com a ingestão atual.
                "columns": [],
                # Metadados adicionais para geração dos modelos dbt.
                "column_metadata": [],
                "primary_key": [],
                "has_updated_at": bool(row.has_updated_at),
            }

        table = tables[table_id]

        table["columns"].append(row.column_name)

        table["column_metadata"].append(
            {
                "name": row.column_name,
                "data_type": row.data_type.lower(),
                "max_length": row.max_length,
                "precision": row.precision,
                "scale": row.scale,
                "nullable": bool(row.is_nullable),
                "is_primary_key": bool(row.is_primary_key),
                "key_ordinal": row.key_ordinal,
            }
        )

        if row.is_primary_key:
            table["primary_key"].append(
                {
                    "ordinal": row.key_ordinal,
                    "name": row.column_name,
                }
            )

    result = []

    for table in tables.values():
        table_name = table["table"]
        has_updated_at = table["has_updated_at"]

        # Ordena a chave composta conforme a ordem declarada no SQL Server.
        primary_key = [
            item["name"]
            for item in sorted(
                table["primary_key"],
                key=lambda item: item["ordinal"],
            )
        ]

        table["primary_key"] = primary_key

        if primary_key:
            table["key_columns"] = primary_key.copy()
            table["key_type"] = "PRIMARY_KEY"

        elif table_name in BUSINESS_KEYS:
            candidate_key = BUSINESS_KEYS[table_name]

            missing_columns = [
                column
                for column in candidate_key
                if column not in table["columns"]
            ]

            if missing_columns:
                raise ValueError(
                    f"Chave de negócio configurada para "
                    f"{table_name} contém colunas inexistentes: "
                    f"{missing_columns}"
                )

            table["key_columns"] = candidate_key.copy()
            table["key_type"] = "BUSINESS_KEY_CANDIDATE"

        else:
            table["key_columns"] = []
            table["key_type"] = "UNRESOLVED"

        table["load_type"] = (
            "INCREMENTAL" if has_updated_at else "FULL"
        )

        table["watermark_column"] = (
            "updated_at" if has_updated_at else None
        )

        result.append(table)

    return sorted(result, key=lambda item: item["table"])


if __name__ == "__main__":
    configs = discover_table_config()

    print(f"Tabelas de negócio encontradas: {len(configs)}")

    for table in configs:
        print(
            f"{table['table']}: "
            f"{len(table['columns'])} colunas | "
            f"{table['key_type']} | "
            f"{table['load_type']}"
        )

    print("\nMetadados de exemplo — tabela brands:")

    brands = next(
        (table for table in configs if table["table"] == "brands"),
        None,
    )

    if brands:
        for column in brands["column_metadata"]:
            print(
                f"  {column['name']}: "
                f"{column['data_type']} | "
                f"nullable={column['nullable']} | "
                f"precision={column['precision']} | "
                f"scale={column['scale']}"
            )

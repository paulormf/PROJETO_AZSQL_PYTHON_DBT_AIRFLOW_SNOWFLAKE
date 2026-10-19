from src.config.table_config import discover_table_config


def main():
    tables = discover_table_config()

    print(f"\nTabelas configuradas: {len(tables)}\n")

    for table in tables:
        key_columns = ", ".join(table["key_columns"])
        if not key_columns:
            key_columns = "NÃO DEFINIDA"

        watermark = table["watermark_column"] or "N/A"

        print(
            f'{table["schema"]}.{table["table"]}'
            f' | Carga: {table["load_type"]}'
            f' | Tipo de chave: {table["key_type"]}'
            f' | Chave: {key_columns}'
            f' | Watermark: {watermark}'
        )


if __name__ == "__main__":
    main()

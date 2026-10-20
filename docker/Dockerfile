FROM apache/airflow:3.3.2-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        unixodbc-dev \
        libgssapi-krb5-2 \
    && if ! dpkg -s msodbcsql18 >/dev/null 2>&1; then \
        curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
          | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
        && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
          > /etc/apt/sources.list.d/microsoft-prod.list \
        && apt-get update \
        && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18; \
    fi \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/requirements-airflow.txt /tmp/requirements-airflow.txt

USER root

RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-snowflake==1.12.3" \
        -r /tmp/requirements-airflow.txt \
    && /opt/dbt-venv/bin/python -c "import pyodbc, dotenv, snowflake.connector; print('Project dependencies installed in dbt-venv')" \
    && chown -R airflow:root /opt/dbt-venv

USER airflow

ENV PATH="/opt/dbt-venv/bin:${PATH}"

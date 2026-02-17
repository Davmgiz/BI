FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir dbt-postgres==1.8.2

COPY . /app

CMD ["bash", "-lc", "dbt --version && while true; do dbt run --profiles-dir profiles; dbt test --profiles-dir profiles || true; sleep 300; done"]

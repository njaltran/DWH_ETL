FROM python:3.11-slim

WORKDIR /app

# Install dbt BigQuery adapter
RUN pip install --no-cache-dir dbt-bigquery==1.8.* 

# Copy project
COPY . /app

# dbt needs a profiles.yml in /root/.dbt by default
# We'll copy one at runtime via an env var, or bake one in.
# Easiest: bake a minimal profiles.yml that uses env vars.
RUN mkdir -p /.dbt

COPY profiles.yml .dbt/profiles.yml

# Default command (you can override in Cloud Run Job)
CMD ["dbt", "run"]
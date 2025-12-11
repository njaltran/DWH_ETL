import duckdb
import numpy as np
import pandas as pd
# Connect to the DuckDB database
conn = duckdb.connect(f"pipeline_name.duckdb")


# Describe the dataset
conn.sql("DESCRIBE").df()

# Fetch all data from 'pokemon' as a DataFrame
table = conn.sql("SELECT * FROM sleep_health.df_data").df()


# Display the DataFrame
print(table)
import duckdb
import numpy as np
import pandas as pd
# Connect to the DuckDB database
conn = duckdb.connect(f"bruh.duckdb")



# Fetch all data from 'pokemon' as a DataFrame
table = conn.sql("SELECT * FROM sleep_health.sleep_health_sheet_resource").df()




# Display the DataFrame
print(table)
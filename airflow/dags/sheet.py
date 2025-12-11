import dlt
import pandas as pd


@dlt.resource(table_name='df_data') 
def sleep_health_sheet():
    csv_url = 'https://docs.google.com/spreadsheets/d/1JVqKCiPHg0HM7-5R4N9aHmROJD-YDomA5tCbBvRT2xw/export?format=csv'
    df = pd.read_csv(csv_url)
    yield df

 

pipeline = dlt.pipeline(
        pipeline_name = "pipeline_name",
        destination = 'duckdb',
        dataset_name = 'sleep_health'
)

info = pipeline.run(sleep_health_sheet())
print(info)
pipeline.dataset().df_data.df()


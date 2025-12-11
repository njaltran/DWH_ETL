import dlt
import pandas as pd


@dlt.source() 
def sleep_health_source():
    @dlt.resource()
    def sleep_health_sheet_resource(table_name: str = "sleep_health"):
        csv_url = 'https://docs.google.com/spreadsheets/d/1JVqKCiPHg0HM7-5R4N9aHmROJD-YDomA5tCbBvRT2xw/export?format=csv'
        df = pd.read_csv(csv_url)
        yield df
    return sleep_health_sheet_resource

 

pipeline = dlt.pipeline(
        pipeline_name = "bruh",
        destination = 'duckdb',
        dataset_name = 'sleep_health'
)

info = pipeline.run(sleep_health_source())
print(info)


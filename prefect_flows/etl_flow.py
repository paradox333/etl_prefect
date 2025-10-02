from prefect import flow
from config import settings
from prefect_flows.tasks.extract import extract_data
from prefect_flows.tasks.transform import transform_data
from prefect_flows.tasks.load import load_data

@flow
def etl_flow(
    bucket: str = settings.BUCKET_NAME,
    file: str = settings.FILE_NAME,
    table: str = settings.TABLE_NAME
    ):
    raw_data = extract_data(bucket, file)
    df = transform_data(raw_data)
    load_data(df, table)

if __name__ == "__main__":
    etl_flow()

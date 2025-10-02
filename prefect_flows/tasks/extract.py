from prefect import task
from prefect_flows.utils.minio_client import get_minio_client

@task
def extract_data(bucket_name: str, file_name: str) -> bytes:
    client = get_minio_client()
    response = client.get_object(bucket_name, file_name)
    data = response.read()
    response.close()
    response.release_conn()
    return data
from dotenv import load_dotenv
import os
# uso solo en local
load_dotenv(override=True)

# Storage Variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET")
# DATABASE Variables
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
DATABASE_SCHEMA = os.getenv("DATABASE_SCHEMA")
# Others Variables
DATABASE_CONN_STR = (
    f"host={DATABASE_HOST} port={DATABASE_PORT} "
    f"dbname={DATABASE_NAME} user={DATABASE_USER} password={DATABASE_PASSWORD} options='-c search_path={DATABASE_SCHEMA}'"
)

COLUMNS_SUMMARIE = ["mt", "bags", "kg"]

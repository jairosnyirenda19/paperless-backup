import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BACKUP_DIR = os.getenv("BACKUP_DIR", "")
DOCS_DIR = os.getenv("DOCS_DIR", "")
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
S3_BUCKET = os.getenv("S3_BUCKET", "")
AWS_REGION = os.getenv("AWS_REGION", "")
LOGS_DIR = os.getenv("LOGS_DIR", "")
PGPASS_FILE = os.path.expanduser("~/.pgpass")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "paperless-backups")
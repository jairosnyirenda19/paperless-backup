import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

PGPASS_FILE = os.path.expanduser("~/.pgpass")

DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")

BACKUP_DIR = os.getenv("BACKUP_DIR", "")
DOCS_DIR   = os.getenv("DOCS_DIR", "")
LOGS_DIR   = os.getenv("LOGS_DIR", "")

STORAGE_DRIVER = os.getenv("STORAGE_DRIVER", "aws")  # Either "minio" or "aws"

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "paperless-backups")

AWS_ACCESS_KEY_ID           = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY       = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION                  = os.getenv("AWS_REGION", "eu-central-1")
AWS_BUCKET                  = os.getenv("AWS_BUCKET", "know-malawi")
AWS_USE_PATH_STYLE_ENDPOINT = False
import os
import subprocess
import gzip
import shutil
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from logger import logger
from config import (
    BACKUP_DIR, DB_NAME, DB_USER, PGPASS_FILE,
    MINIO_BUCKET as S3_BUCKET,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    DOCS_DIR
)

DATE = datetime.now().strftime("%Y%m%d%H%M%S")

def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Command failed: {cmd}\n{result.stderr}")
        raise Exception(result.stderr)
    return result.stdout.strip()

def backup_db():
    sql_file = os.path.join(BACKUP_DIR, f"db_backup_{DATE}.sql")
    gz_file = f"{sql_file}.gz"
    # os.environ['PGPASSFILE'] = os.path.expanduser("~/.pgpass")
    cmd = f"PGPASSFILE={PGPASS_FILE} pg_dump -U {DB_USER} {DB_NAME} -h localhost -f {sql_file}"
    run_cmd(cmd)

    with open(sql_file, 'rb') as f_in, gzip.open(gz_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    os.remove(sql_file)
    return gz_file

def incremental_upload(local_path, s3_client, s3_prefix="media/"):
    for root, dirs, files in os.walk(local_path):
        for file in files:
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, local_path)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")

            try:
                obj_stat = s3_client.stat_object(S3_BUCKET, s3_key)
                local_mtime = int(os.path.getmtime(full_path))
                remote_mtime = int(obj_stat.last_modified.timestamp())
                if local_mtime <= remote_mtime:
                    continue  # Not modified
            except S3Error as e:
                if e.code != "NoSuchKey":
                    logger.error(f"Error checking {s3_key}: {str(e)}")
                    continue  # Skip problematic files

            s3_client.fput_object(S3_BUCKET, s3_key, full_path)
            logger.info(f"Uploaded {s3_key}")

def main():
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)

        # Backup DB
        logger.info("Starting DB backup...")
        db_gz = backup_db()

        # MinIO Client
        s3 = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False  # Change to True if using HTTPS
        )

        # Create bucket if it doesn't exist
        if not s3.bucket_exists(S3_BUCKET):
            s3.make_bucket(S3_BUCKET)

        # Upload DB backup
        db_key = f"db/{os.path.basename(db_gz)}"
        s3.fput_object(S3_BUCKET, db_key, db_gz)
        logger.info(f"Uploaded DB backup: {db_key}")

        # Upload documents
        logger.info("Starting media upload...")
        incremental_upload(DOCS_DIR, s3)

        logger.info("Backup completed successfully!")

    except Exception as e:
        logger.error(f"Backup failed: {str(e)}")
        # You can add Telegram or email notifications here

if __name__ == "__main__":
    main()

import os
import subprocess
import gzip
import shutil
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from logger import logger
from config import (
    BACKUP_DIR, DB_NAME, DB_USER, PGPASS_FILE,
    DOCS_DIR,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_BUCKET,
    STORAGE_DRIVER
)

DATE = datetime.now().strftime("%Y%m%d%H%M%S")

def validate_config():
    """Validate configuration before starting backup."""
    logger.info("Validating configuration...")
    
    # Common validations
    required_vars = ['BACKUP_DIR', 'DB_NAME', 'DB_USER', 'PGPASS_FILE', 'DOCS_DIR', 'STORAGE_DRIVER']
    for var in required_vars:
        if not globals().get(var):
            raise ValueError(f"Missing required configuration: {var}")
    
    # Storage-specific validations
    if STORAGE_DRIVER == "aws":
        aws_vars = [
            ('AWS_ACCESS_KEY_ID', AWS_ACCESS_KEY_ID),
            ('AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY),
            ('AWS_REGION', AWS_REGION),
            ('AWS_BUCKET', AWS_BUCKET)
        ]
        missing_aws = [name for name, value in aws_vars if not value]
        if missing_aws:
            raise ValueError(f"Missing AWS configuration: {', '.join(missing_aws)}")
            
    elif STORAGE_DRIVER == "minio":
        minio_vars = [
            ('MINIO_ENDPOINT', MINIO_ENDPOINT),
            ('MINIO_ACCESS_KEY', MINIO_ACCESS_KEY),
            ('MINIO_SECRET_KEY', MINIO_SECRET_KEY),
            ('MINIO_BUCKET', MINIO_BUCKET)
        ]
        missing_minio = [name for name, value in minio_vars if not value]
        if missing_minio:
            raise ValueError(f"Missing MinIO configuration: {', '.join(missing_minio)}")
    else:
        raise ValueError(f"Unsupported STORAGE_DRIVER: {STORAGE_DRIVER}. Must be 'aws' or 'minio'")
    
    # Check if directories exist
    if not os.path.exists(DOCS_DIR):
        logger.warning(f"DOCS_DIR does not exist: {DOCS_DIR}")
    
    if not os.path.exists(PGPASS_FILE):
        logger.warning(f"PGPASS_FILE does not exist: {PGPASS_FILE}")
    
    logger.info("Configuration validation passed")


def run_cmd(cmd):
    """Execute shell command with better error handling."""
    logger.debug(f"Executing command: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"Command failed with return code {result.returncode}: {cmd}")
            logger.error(f"STDERR: {result.stderr}")
            logger.error(f"STDOUT: {result.stdout}")
            raise Exception(f"Command failed: {result.stderr}")
        logger.debug(f"Command output: {result.stdout[:200]}...")
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        logger.error(f"Command timed out: {cmd}")
        raise Exception("Command timed out")


def backup_db():
    """Create compressed database backup."""
    logger.info(f"Starting database backup for {DB_NAME}...")
    
    sql_file = os.path.join(BACKUP_DIR, f"db_backup_{DATE}.sql")
    gz_file = f"{sql_file}.gz"
    
    # Check if pg_dump is available
    try:
        run_cmd("pg_dump --version")
    except Exception:
        raise Exception("pg_dump is not available. Please install PostgreSQL client tools.")
    
    # Create database dump
    cmd = f"PGPASSFILE={PGPASS_FILE} pg_dump -U {DB_USER} {DB_NAME} -h localhost -f {sql_file}"
    try:
        run_cmd(cmd)
        
        # Check if SQL file was created and has content
        if not os.path.exists(sql_file):
            raise Exception("SQL dump file was not created")
        
        file_size = os.path.getsize(sql_file)
        if file_size == 0:
            raise Exception("SQL dump file is empty")
        
        logger.info(f"Database dump created: {sql_file} ({file_size} bytes)")
        
        # Compress the SQL file
        with open(sql_file, 'rb') as f_in:
            with gzip.open(gz_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Remove uncompressed file
        os.remove(sql_file)
        
        compressed_size = os.path.getsize(gz_file)
        logger.info(f"Database backup compressed: {gz_file} ({compressed_size} bytes)")
        
        return gz_file
        
    except Exception as e:
        # Clean up partial files
        for file_path in [sql_file, gz_file]:
            if os.path.exists(file_path):
                os.remove(file_path)
        raise Exception(f"Database backup failed: {str(e)}")


def get_s3_client():
    """Create S3 client based on storage driver."""
    logger.info(f"Initializing {STORAGE_DRIVER} client...")
    
    if STORAGE_DRIVER == "aws":
        try:
            session = boto3.session.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION
            )
            client = session.client('s3')
            
            # Testing the connection
            try:
                client.head_bucket(Bucket=AWS_BUCKET)
                logger.info("AWS S3 connection successful (via head_bucket)")
            except NoCredentialsError:
                raise Exception("AWS credentials are invalid or not found")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ('403', 'AccessDenied'):
                    raise Exception("Access denied: check if the bucket exists and you have access")
                elif error_code == '404':
                    raise Exception("Bucket does not exist or is not accessible")
                elif error_code == 'InvalidAccessKeyId':
                    raise Exception("AWS Access Key ID is invalid")
                elif error_code == 'SignatureDoesNotMatch':
                    raise Exception("AWS Secret Access Key is invalid")
                else:
                    raise Exception(f"AWS connection failed: {e}")
            
            return client
            
        except Exception as e:
            logger.error(f"Failed to create AWS S3 client: {str(e)}")
            raise
            
    elif STORAGE_DRIVER == "minio":
        try:
            # Determine if endpoint should use HTTPS
            secure = MINIO_ENDPOINT.startswith('https://') or ':443' in MINIO_ENDPOINT
            
            # Clean endpoint (remove protocol if present)
            endpoint = MINIO_ENDPOINT.replace('https://', '').replace('http://', '')
            
            client = Minio(
                endpoint,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=secure
            )
            
            # Test connection
            try:
                list(client.list_buckets())
                logger.info("MinIO connection successful")
            except S3Error as e:
                if 'InvalidAccessKeyId' in str(e):
                    raise Exception("MinIO Access Key is invalid")
                elif 'SignatureDoesNotMatch' in str(e):
                    raise Exception("MinIO Secret Key is invalid")
                else:
                    raise Exception(f"MinIO connection failed: {e}")
            
            return client
            
        except Exception as e:
            logger.error(f"Failed to create MinIO client: {str(e)}")
            raise
    else:
        raise ValueError(f"Unsupported STORAGE_DRIVER: {STORAGE_DRIVER}")


def get_bucket_name():
    """Get bucket name based on storage driver."""
    if STORAGE_DRIVER == "aws":
        return AWS_BUCKET
    elif STORAGE_DRIVER == "minio":
        return MINIO_BUCKET
    else:
        raise ValueError(f"Unsupported STORAGE_DRIVER: {STORAGE_DRIVER}")


def ensure_bucket_exists(s3, bucket_name):
    """Ensure bucket exists, create if necessary."""
    logger.info(f"Checking if bucket '{bucket_name}' exists...")
    
    if STORAGE_DRIVER == "aws":
        try:
            s3.head_bucket(Bucket=bucket_name)
            logger.info(f"AWS bucket '{bucket_name}' exists")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"Creating AWS bucket '{bucket_name}'...")
                try:
                    if AWS_REGION == 'us-east-1':
                        s3.create_bucket(Bucket=bucket_name)
                    else:
                        s3.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                        )
                    logger.info(f"AWS bucket '{bucket_name}' created successfully")
                except ClientError as create_error:
                    raise Exception(f"Failed to create AWS bucket: {create_error}")
            elif error_code == '403':
                raise Exception(f"Access denied to AWS bucket '{bucket_name}'. Check permissions.")
            else:
                raise Exception(f"Error checking AWS bucket: {e}")
    else:
        try:
            if not s3.bucket_exists(bucket_name):
                logger.info(f"Creating MinIO bucket '{bucket_name}'...")
                s3.make_bucket(bucket_name)
                logger.info(f"MinIO bucket '{bucket_name}' created successfully")
            else:
                logger.info(f"MinIO bucket '{bucket_name}' exists")
        except S3Error as e:
            raise Exception(f"Error with MinIO bucket: {e}")


def upload_file(s3, bucket_name, key, path):
    """Upload file to S3/MinIO."""
    logger.debug(f"Uploading {path} to {key}")
    
    if not os.path.exists(path):
        raise Exception(f"File does not exist: {path}")
    
    file_size = os.path.getsize(path)
    logger.debug(f"File size: {file_size} bytes")
    
    try:
        if STORAGE_DRIVER == "aws":
            s3.upload_file(path, bucket_name, key)
        else:
            s3.fput_object(bucket_name, key, path)
        logger.debug(f"Successfully uploaded {key}")
    except Exception as e:
        raise Exception(f"Upload failed for {key}: {str(e)}")


def object_exists_and_modified(s3, bucket_name, key, path):
    """Check if local file is newer than remote object."""
    try:
        if STORAGE_DRIVER == "aws":
            try:
                response = s3.head_object(Bucket=bucket_name, Key=key)
                local_mtime = int(os.path.getmtime(path))
                remote_mtime = int(response['LastModified'].timestamp())
                return local_mtime > remote_mtime
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    return True  # Object doesn't exist, needs upload
                raise
        else:
            try:
                obj = s3.stat_object(bucket_name, key)
                local_mtime = int(os.path.getmtime(path))
                remote_mtime = int(obj.last_modified.timestamp())
                return local_mtime > remote_mtime
            except S3Error as e:
                if e.code == "NoSuchKey":
                    return True  # Object doesn't exist, needs upload
                raise
    except Exception as e:
        logger.warning(f"Error checking object {key}: {str(e)}")
        return True  # If we can't check, assume we need to upload


def incremental_upload(local_path, s3_client, bucket_name, s3_prefix="media/"):
    """Upload files incrementally based on modification time."""
    logger.info(f"Starting incremental upload from {local_path}")
    
    if not os.path.exists(local_path):
        logger.warning(f"Local path does not exist: {local_path}")
        return
    
    upload_count = 0
    skip_count = 0
    error_count = 0
    
    for root, dirs, files in os.walk(local_path):
        for file in files:
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, local_path)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            
            try:
                if object_exists_and_modified(s3_client, bucket_name, s3_key, full_path):
                    upload_file(s3_client, bucket_name, s3_key, full_path)
                    logger.info(f"Uploaded {s3_key}")
                    upload_count += 1
                else:
                    logger.debug(f"Skipped {s3_key} (not modified)")
                    skip_count += 1
            except Exception as e:
                logger.error(f"Error uploading {s3_key}: {str(e)}")
                error_count += 1
    
    logger.info(f"Upload summary - Uploaded: {upload_count}, Skipped: {skip_count}, Errors: {error_count}")


def main():
    try:
        logger.info("="*50)
        logger.info("Starting backup process...")
        logger.info(f"Timestamp: {DATE}")
        
        # Validate configuration
        validate_config()
        
        # Create backup directory
        os.makedirs(BACKUP_DIR, exist_ok=True)
        logger.info(f"Backup directory: {BACKUP_DIR}")
        
        # Database backup
        logger.info("Starting database backup...")
        db_gz = backup_db()
        
        # Initialize storage client
        logger.info(f"Using storage backend: {STORAGE_DRIVER}")
        s3 = get_s3_client()
        bucket_name = get_bucket_name()
        
        # Ensure bucket exists
        ensure_bucket_exists(s3, bucket_name)
        
        # Upload database backup
        db_key = f"db/{os.path.basename(db_gz)}"
        logger.info(f"Uploading database backup to {db_key}...")
        upload_file(s3, bucket_name, db_key, db_gz)
        logger.info(f"Database backup uploaded successfully: {db_key}")
        
        # Upload media files
        logger.info("Starting media file upload...")
        incremental_upload(DOCS_DIR, s3, bucket_name)
        
        logger.info("="*50)
        logger.info("Backup completed successfully!")
        
    except Exception as e:
        logger.error("="*50)
        logger.error(f"Backup failed: {str(e)}")
        logger.error("="*50)
        # raise


if __name__ == "__main__":
    main()
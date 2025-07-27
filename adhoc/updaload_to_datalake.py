from minio import Minio
from minio.error import S3Error


def set_minio_client() -> Minio:
    # Connect to local MinIO instance
    client = Minio(
        "localhost:9000",  # or "127.0.0.1:9000"…
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False  # Use True if using HTTPS
    )
    return client


def upload_to_datalake() -> None:
    """
    Uploads a file to a MinIO bucket.
    """
    client = set_minio_client()

    # Check if the client is connected
    try:
        client.list_buckets()
        print("Connected to MinIO server.")
    except S3Error as e:
        print(f"Error connecting to MinIO: {e}")
        return

    # Ensure bucket exists
    bucket_name = "data-lake"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Upload a file (e.g. CSV or dataset)
    client.fput_object(bucket_name, "my_dataset.csv", "/Users/arjel.miras/PycharmProjects/DA_Project/Sales-Data-Analytics-Pipeline-Service/sql/data.csv")
    print("Upload successful!")


def drop_from_datalake() -> None:
    """
    Drops a file from a MinIO bucket.
    """
    client = set_minio_client()
    bucket_name = "data-lake"
    object_name = "my_dataset_updated.csv"

    try:
        client.remove_object(bucket_name, object_name)
        print(f"Object {object_name} deleted successfully from bucket {bucket_name}.")
    except S3Error as e:
        print(f"Error deleting object: {e}")


def create_bucket(bucket_name: str) -> None:
    """
    Creates a bucket in MinIO.
    """
    client = set_minio_client()

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        print(f"Error creating bucket: {e}")


def drop_bucket(bucket_name: str) -> None:
    """
    Drops a bucket in MinIO.
    """
    client = set_minio_client()

    try:
        if client.bucket_exists(bucket_name):
            client.remove_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' deleted successfully.")
        else:
            print(f"Bucket '{bucket_name}' does not exist.")
    except S3Error as e:
        print(f"Error deleting bucket: {e}")


if __name__ == "__main__":
    # create bucket
    # create_bucket("temp-bucket")

    # upload
    upload_to_datalake()

    # drop
    # drop_from_datalake()

    # drop bucket
    # drop_bucket("temp-bucket")

import boto3
import os
import multiprocessing
from multiprocessing import Queue, Value, Lock
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Set your AWS details and local path
BUCKET_NAME = 'poc-flooring-price'
DOWNLOAD_PATH = '/Volumes/shared/dump/S3/'  # Local path to save downloaded files
DOWNLOAD_BATCH_SIZE = 50  # Number of downloads before refreshing credentials


# Function to get new AWS session and client
def get_s3_client():
    session = boto3.Session()  # You can add parameters like profile_name if needed
    return session.client('s3')


# Function to list all files in the S3 bucket
def list_files(s3_client, bucket_name):
    file_list = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get('Contents', []):
            file_list.append(obj['Key'])
    return file_list


# Function to download a single file
def download_file(q, download_count, client_lock):
    s3_client = get_s3_client()  # Refresh AWS client within each process to ensure independent handling

    while True:
        with client_lock:
            if q.empty():
                break
            file_key = q.get()

        try:
            # Refresh credentials after every DOWNLOAD_BATCH_SIZE downloads
            with client_lock:
                if download_count.value >= DOWNLOAD_BATCH_SIZE:
                    print("Refreshing AWS credentials...")
                    s3_client = get_s3_client()  # Refresh credentials
                    download_count.value = 0

            file_path = os.path.join(DOWNLOAD_PATH, file_key.split('/')[-1])
            s3_client.download_file(BUCKET_NAME, file_key, file_path)
            print(f"Downloaded: {file_key}")

            # Print queue size whenever an item is retrieved


            # Increment the download count safely
            with client_lock:
                download_count.value += 1
                print(download_count.value)

        except Exception as e:
            print(f"Error downloading {file_key}: {e}")


# Function to handle credential refresh and multiprocessing downloads
def download_files_concurrently(file_keys):
    # Create a multiprocessing queue and add all file keys to it
    q = Queue()
    for key in file_keys:
        q.put(key)

    download_count = Value('i', 0)  # Shared variable for counting downloads
    client_lock = Lock()  # Lock for synchronizing credential refreshes and download count

    # Create a pool of worker processes
    processes = []
    for _ in range(4):
        p = multiprocessing.Process(target=download_file, args=(q, download_count, client_lock))
        p.start()
        processes.append(p)

    # Wait for all processes to complete
    for p in processes:
        p.join()


def main():
    try:
        s3_client = get_s3_client()  # Initial client
        print("Fetching file list...")
        file_keys = list_files(s3_client, BUCKET_NAME)
        print(f"Total files found: {len(file_keys)}")

        print("Starting download...")
        download_files_concurrently(file_keys)
        print("Download completed.")
    except (NoCredentialsError, PartialCredentialsError):
        print("AWS credentials are missing or incomplete. Please configure your AWS access.")


if __name__ == "__main__":
    main()

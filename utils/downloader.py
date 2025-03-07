import csv
import re
import requests
import os
import boto3
from multiprocessing import Queue, Process, cpu_count
from urllib.parse import unquote
import gc

SHOW_NAME = 'momma'
BASE_URL = "https://dn720003.ca.archive.org/0/items/mamas-family_202311/"
AWS_REGION = "us-east-1"  # Change to your desired region
S3_BUCKET_NAME = "poc-flooring-price"  # Replace with your S3 bucket name


def save_content(response, output_file, output_dir="downloads"):
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        file_path = os.path.join(output_dir, output_file)

        # Write the response content in chunks
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    file.write(chunk)

        print(f"Saved {file_path}")
        return file_path
    except IOError as e:
        print(f"Failed to save content to {output_file}: {e}")
        return None


def download_worker(queue):
    while True:
        item = queue.get()
        if item is None:
            break  # Exit if a sentinel (None) is found

        url, output_file = item
        try:
            print(f"GETTING: {unquote(url)}")
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an error on a bad status code

            # Stream the content directly to disk using the response object
            file_path = save_content(response, output_file)

            if file_path:
                # Post download: Upload to S3 using new AWS credentials
                upload_to_s3(file_path)

        except requests.exceptions.RequestException as e:
            print(f"Failed to download {unquote(url)}: {e}")
        finally:
            # Clean up to free memory
            del response
            del item
            gc.collect()  # Manually trigger garbage collection

def extract_episode_code(url):
    try:
        match = re.search(r'S\d{2}E\d{2}', url)
        if match:
            return f"{SHOW_NAME}-{match.group(0)}.mp4"
        else:
            return None
    except re.error as e:
        print(f"Failed to extract episode code from {url}: {e}")
        return None


def get_new_aws_credentials():
    try:
        # First request to get the token
        token_response = requests.put(
            'http://169.254.169.254/latest/api/token',
            headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'},
            timeout=5
        )
        token_response.raise_for_status()
        token = token_response.text

        # Second request with the token
        response = requests.get(
            'http://169.254.169.254/latest/meta-data/iam/security-credentials/flooring_poc',
            headers={'X-aws-ec2-metadata-token': token},
            timeout=5
        )
        response.raise_for_status()
        credentials = response.json()
        return credentials
    except requests.exceptions.RequestException as e:
        print(f"Failed to get AWS credentials: {e}")
        return None


def upload_to_s3(file_path):
    try:
        # Get new AWS credentials
        credentials = get_new_aws_credentials()
        if not credentials:
            print("Failed to retrieve AWS credentials. Aborting upload.")
            return

        # Create a new S3 client with the temporary credentials
        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['Token']
        )

        # Upload the file to S3
        s3_client.upload_file(file_path, S3_BUCKET_NAME, os.path.basename(file_path))
        print(f"Uploaded {file_path} to {S3_BUCKET_NAME}")

        # Clean up the local file after upload
        os.remove(file_path)
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Failed to upload {file_path} to S3: {e}")
    except boto3.exceptions.Boto3Error as e:
        print(f"Boto3 error occurred: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while uploading to S3: {e}")


def process_csv(csv_file, num_processes=5):
    queue = Queue()

    try:
        # Enqueue URLs
        with open(csv_file, mode='r') as file:
            reader = csv.reader(file)
            for row in reader:
                if row:  # Ensure the row is not empty
                    url = f"{BASE_URL}{row[0]}"
                    output_file = extract_episode_code(row[0])
                    if output_file:
                        queue.put((url, output_file))
    except IOError as e:
        print(f"Failed to read CSV file {csv_file}: {e}")
        return

    # Start the processes
    processes = []
    for i in range(num_processes):
        p = Process(target=download_worker, args=(queue,))
        p.start()
        processes.append(p)

    # Enqueue None to signal processes to exit
    for _ in range(num_processes):
        queue.put(None)

    # Wait for all processes to complete
    for p in processes:
        p.join()


# Example usage
if __name__ == "__main__":
    csv_file = './data.csv'
    process_csv(csv_file, num_processes=cpu_count())  # Adjust the number of processes as needed

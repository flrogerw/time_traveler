#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path
from urllib.parse import urlparse
from psycopg2.extras import execute_values, DictCursor
import psycopg2
import logging
from multiprocessing import Pool
from done import done_episodes

BASE_PATH = '/Volumes/shared/time_traveler/'


# Sample list of shows
def get_db_connection() -> psycopg2.extensions.connection:
    """Establish and return a database connection."""
    try:
        conn = psycopg2.connect(
            database="time_traveler",
            user="postgres",
            password="m06Ar14u",
            host="192.168.1.201",
            port=5432
        )
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise


def get_show_ids():
    with get_db_connection() as db:
        cur = db.cursor(cursor_factory=DictCursor)
        cur.execute("""SELECT show_id FROM shows where show_id NOT IN (1,173,181) AND show_id > 87""")
    return sum(cur.fetchall(), [])

def get_episodes(show_id):
    with get_db_connection() as db:
        cur = db.cursor(cursor_factory=DictCursor)
        cur.execute("""SELECT DISTINCT(episode_file), * FROM episodes where show_id = %s""", (show_id,))
        formatted_records = [{**dict(record)} for record in cur.fetchall()]
    return formatted_records


def process_mp4(input_file):
    """Process a single MP4 file using FFmpeg."""
    print(input_file)
    a = urlparse(input_file)
    file = Path(os.path.basename(a.path))
    filename = str(file.with_suffix(''))
    parent_path = Path(input_file).parent

    output_file = f"{parent_path}/{filename}.build.mp4"
    process_cmd = [
        'ffmpeg', '-loglevel', 'quiet', '-i', input_file,
        '-r', '30',
        '-c:v', 'h264_videotoolbox',
        '-preset', 'slow', "-crf", '22',
        output_file, '-y'
    ]

    subprocess.run(process_cmd)
    Path(output_file).rename(input_file)
    return True


def process_show_episode(show_id):
    """Process all episodes for a given show."""
    episodes = get_episodes(show_id)
    files_to_process = []

    for episode in episodes:
        d = episode['episode_airdate']
        year = int(str(d.year)[2:])
        decade = f"{year - (year % 10)}s"
        file_str = f"{BASE_PATH}{decade}/{year}/{episode['episode_file']}"
        files_to_process.append(file_str)
    return files_to_process


def main():
    """Main function to run the multiprocessing pool."""
    shows_to_process = get_show_ids()

    for show_id in shows_to_process:
        all_files_to_process = []
        print(show_id)
        all_files_to_process.extend(process_show_episode(show_id))
        all_files_to_process = [elem for elem in all_files_to_process if elem not in done_episodes]
        if len(all_files_to_process) > 0:
            # Set up the multiprocessing pool
            num_workers = min(4, len(all_files_to_process))  # Use as many CPUs as available
            with Pool(num_workers) as pool:
                pool.map(process_mp4, all_files_to_process)


if __name__ == '__main__':
    main()

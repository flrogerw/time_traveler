import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    'dbname': 'time_traveler',
    'user': 'postgres',
    'password': 'm06Ar14u',
    'host': '192.168.1.201',
    'port': 5432,
}

ROOT_DIR = '/Volumes/TTBS/time_traveler'
TABLE_NAME = 'episodes'
COLUMN_NAME = 'episode_file'

YEAR_FOLDER_PATTERN = re.compile(r'^\d{2}$')

def get_file_path(airdate, episode_file):
    year = int(airdate.strftime("%y"))
    decade = f"{(year // 10) % 10}0s"
    return f'{ROOT_DIR}/{decade}/{airdate.year % 100:02d}/{episode_file}'


def get_db_filepaths():
    query = f"SELECT * FROM {TABLE_NAME};"
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    conn.close()

    filepaths = set()
    for row in result:
        fname = row['episode_file']
        if fname.lower().endswith('.mp4'):
            candidate = get_file_path(row['episode_airdate'], row['episode_file'])
            filepaths.add(candidate)
    return filepaths


def get_filesystem_filenames():
    fs_filenames = set()
    for decade in sorted(os.listdir(ROOT_DIR)):
        decade_path = os.path.join(ROOT_DIR, decade)
        if not os.path.isdir(decade_path) or not decade.endswith('s'):
            continue

        for year in sorted(os.listdir(decade_path)):
            if not YEAR_FOLDER_PATTERN.match(year):
                continue

            year_path = os.path.join(decade_path, year)
            if not os.path.isdir(year_path):
                continue

            for item in os.listdir(year_path):
                if item.lower().endswith('.mp4'):
                    fs_filenames.add(f"{year_path}/{item}")
    return fs_filenames


def compare_files():
    db_filenames = get_db_filepaths()
    fs_filenames = get_filesystem_filenames()

    # --- Files on disk but not in DB ---
    missing_in_db = fs_filenames - db_filenames
    if missing_in_db:
        print("\nFiles on disk but missing in DB:")
        for f in sorted(missing_in_db):
            print("  ", f)
            # os.remove(f)
    else:
        print("\nNo files are missing in DB.")

    # --- Files in DB but not on disk ---
    missing_on_disk = db_filenames - fs_filenames
    if missing_on_disk:
        print("\nFiles in DB but missing on disk:")
        for f in sorted(missing_on_disk):
           print("  ", f)
    else:
        print("\nNo files are missing on disk.")


if __name__ == "__main__":
    compare_files()

import os
import re
import shutil
import psycopg2

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


# Matches only 2-digit folder names (e.g., '60', '01', '99')
YEAR_FOLDER_PATTERN = re.compile(r'^\d{2}$')


def get_db_filenames():
    query = f"SELECT {COLUMN_NAME} FROM {TABLE_NAME};"
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return set(row[0] for row in result if row[0].lower().endswith('.mp4'))


def find_missing_mp4_files():
    db_filenames = get_db_filenames()

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

            # Only scan .mp4 files directly inside the year folder
            for item in os.listdir(year_path):
                file_path = os.path.join(year_path, item)
                if os.path.isfile(file_path) and item.lower().endswith('.mp4'):
                    if item not in db_filenames:
                        size_mb = os.path.getsize(file_path) / (1024 * 1024)
                        print(f"{file_path} - {size_mb:.2f} MB")



if __name__ == "__main__":
    find_missing_mp4_files()

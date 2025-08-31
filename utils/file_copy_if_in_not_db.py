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
DUMP_ROOT = '/Volumes/TTBS/dump'
TABLE_NAME = 'episodes'
COLUMN_NAME = 'episode_file'
FILTER_PREFIX = 'In_Search_Of'  # Example: 'I_Love_Lucy' or leave blank to disable filtering

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

def find_and_copy_missing_mp4_files():
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

            for item in os.listdir(year_path):
                if not item.lower().endswith('.mp4'):
                    continue

                if FILTER_PREFIX and not item.startswith(FILTER_PREFIX):
                    continue

                file_path = os.path.join(year_path, item)
                if os.path.isfile(file_path) and item not in db_filenames:
                    dump_dir = os.path.join(DUMP_ROOT, FILTER_PREFIX)
                    os.makedirs(dump_dir, exist_ok=True)

                    dest_path = os.path.join(dump_dir, item)
                    if os.path.exists(dest_path):
                        print(f"Skipping (already exists): {dest_path}")
                        continue

                    size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    print(f"Copying: {file_path} - {size_mb:.2f} MB")
                    shutil.copy2(file_path, dest_path)

if __name__ == "__main__":
    find_and_copy_missing_mp4_files()

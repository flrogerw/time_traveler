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
FILTER_PREFIX = 'Dream_of_Jeannie'  # Example: 'I_Love_Lucy' or leave blank to disable filtering

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

def delete_existing_files_from_dump():
    db_filenames = get_db_filenames()

    for root, _, files in os.walk(DUMP_ROOT):
        for filename in files:
            if filename.lower().endswith('.mp4') and filename in db_filenames:
                full_path = os.path.join(root, filename)
                print(f"Deleting {full_path} (already in DB)")
                os.remove(full_path)


if __name__ == "__main__":
    delete_existing_files_from_dump()

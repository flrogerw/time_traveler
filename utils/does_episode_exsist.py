import glob
import string
import urllib
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime
import os.path
import psycopg2
import subprocess
import json

# Example connection parameters (update with your connection details)
conn = psycopg2.connect(database="time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.201", port=5432)

# Create a cursor object
cur = conn.cursor()
ROOT_PATH = "/Volumes/shared/time_traveler"
translator = str.maketrans('', '', string.punctuation)

def get_file_name(url_string):
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    filename = str(file.with_suffix('')).translate(translator).lower().strip()
    filename = filename.replace(' ', '')
    return filename

def get_meta_title(file_path):
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json',
        '-show_entries', 'format_tags=title', file_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    metadata = json.loads(result.stdout)
    title = metadata.get('format', {}).get('tags', {}).get('title', None)
    return title

def remove_black_bars(file_path):



    cmd = [
         'ffmpeg', '-i', file_path, '-vf', '"crop=768:576:128:0"', '-c:v', 'libx264', '-preset slow', '-crf', 22, '-c:a', 'copy', f"{file_path}_tmp"
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    metadata = json.loads(result.stdout)
    title = metadata.get('format', {}).get('tags', {}).get('title', None)
    return title



query = "Select episode_file, episode_airdate, episode_id from episodes;"
cur.execute(query)
delete_ids = []
for f in cur.fetchall():
    file_path, airdate, id = f
    year = int(str(airdate.year)[2:])
    decade = f"{year - (year % 10)}s"
    # meta_title = get_meta_title(f"{ROOT_PATH}/{decade}/{year}/{file_path}")
    # if meta_title:
    #    print(f"{year}/{file_path} -- {meta_title}")
    if not os.path.exists(f"{ROOT_PATH}/{decade}/{year}/{file_path}"):
        delete_ids.append(id)
if len(delete_ids) > 0:
    #cur.execute(f"DELETE FROM episodes where episode_id in ({','.join(map(str, delete_ids))})")
    #conn.commit()
    print('DELETED: ', ','.join(map(str, delete_ids)))
else:
    print('NO MIS-MATCHES')


"""
for file in glob.glob(f"/Volumes/shared/dump/ANGELS_PILOT*.mp4"):
    failed = []
    url_string = urllib.parse.unquote(file)
    filename = get_file_name(url_string)
    print(url_string, get_meta_title(url_string))
"""
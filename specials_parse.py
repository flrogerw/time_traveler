import hashlib
import os
import re
import shutil
from urllib.parse import urlparse

import psycopg2
import string
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import json
import subprocess
import html


BLACKOUT_DURATION = 1.8

IMDB_URL = 'https://www.imdb.com/title/'
FILES_DIR = '/Volumes/TTBS/dump/specials/'
DEV_MODE = False

translator = str.maketrans('', '', string.punctuation)
con = psycopg2.connect(database="time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.201", port=5432)
cur = con.cursor()

with open('specials.json', 'r') as file:
    specials = [tuple(item) for item in json.load(file)]


# specials = [("The good the bad and the ugly Leone.mp4", "tt0060196", {"name":  "The Good, the Bad and the Ugly"})]
def process_remove_bars(input_file):
    a = urlparse(input_file)
    file = Path(os.path.basename(a.path))
    filename = str(file.with_suffix(''))
    parent_path = Path(input_file).parent
    cropdetect_cmd = [
        'ffmpeg', '-i', input_file, '-vf', 'cropdetect', '-f', 'null', '-'
    ]
    result = subprocess.run(cropdetect_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    counter = {}
    for line in result.stderr.splitlines():
        match = re.search(r'crop=(\d+:\d+:\d+:\d+)', line)
        if match:
            hash_str = hashlib.md5(str(match.group(1)).encode()).hexdigest()
            counter[hash_str] = counter.get(hash_str, []) + [match.group(1)]

    largest_list_key = max(counter, key=lambda k: len(counter[k]))
    crop_values = counter[largest_list_key].pop()

    if crop_values:
        output_file = f"{parent_path}/{filename}.build.mp4"
        crop_cmd = [
            'ffmpeg', '-y', '-loglevel', 'quiet', '-i', input_file,
            '-r', '30',
            '-vf', f'crop={crop_values},scale=640:480,setdar=4/3',
            '-af', 'loudnorm=I=-26:TP=-2:LRA=7',
            '-b:v', '500k',
            '-c:v', 'h264_videotoolbox',
            '-c:a', 'aac',
            '-b:a', '128k',
            output_file
        ]
        subprocess.run(crop_cmd)
        return output_file
    return None



def get_blackout(filename):
    result_times = []
    logfile = "./FFMPEGLOG.txt"
    subprocess.call(
        f'ffmpeg -i "{filename}" -vf "blackdetect=d={BLACKOUT_DURATION}:pix_th=0.05" -an -f null - 2>&1 | grep blackdetect > {logfile}',
        shell=True)
    with open(logfile, 'r') as log_file:
        for row in log_file:
            if 'black_start' in row:
                deltas = row.split()[3:]
                start = float(deltas[0].split(':')[1])
                end = float(deltas[1].split(':')[1])
                result_times.append((start, end))
    return result_times

def get_length(filename):
    result = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries",
                             "format=duration", "-of",
                             "default=noprint_wrappers=1:nokey=1", filename],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    return float(result.stdout)




headers = {'User-Agent': 'Mozilla/5.0'}

for special in specials:
    file_name, imdb_id, extras = special
    page = requests.get(f"{IMDB_URL}{imdb_id}", headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")

    title = soup.find_all("span", class_="hero__primary-text")[0].text
    year = soup.find_all("script", type="application/ld+json")[0].text

    data = json.loads("".join(soup.find("script", {"type": "application/ld+json"}).contents))
    release_date = extras['movie_release_date'] if 'movie_release_date' in extras else data['datePublished'][:4]
    data['contentRating'] = 'Not Rated' if 'contentRating' not in data else data['contentRating']
    data['name'] = extras['name'] if 'name' in extras else data['name']
    year = int(str(release_date)[2:])
    decade = f"{str((year - (year % 10))).zfill(2)}s"
    new_file = f"{html.unescape(data['name']).translate(translator)}_{release_date}"
    new_file = new_file.replace(' ', '_')
    special_end_point = int(get_length(f"{FILES_DIR}{file_name}"))
    save_file = f"/Volumes/TTBS/time_traveler/{decade}/specials/{new_file}.mp4"
    print(save_file)
    description = html.unescape(data['description']) if 'description' in data else html.unescape(data['name'])

    insert_dict = {
        'specials_file': f"{new_file}.mp4",
        'specials_title': html.unescape(data['name']),
        'specials_description': description,
        'specials_genre': ', '.join(data['genre']),
        'specials_airdate': release_date,
        'specials_stars': ', '.join([record['name'] for record in data['actor']]),
        'start_point': 0,
        'end_point': special_end_point,
        'specials_rating': 'G' if data['contentRating'] == 'Approved' else data['contentRating']
    }

    if 'special_season' in extras:
        insert_dict['special_season'] = extras['special_season']

    if DEV_MODE:
        print(insert_dict)
    else:
        out_file = process_remove_bars(f"{FILES_DIR}{file_name}")
        shutil.copy(out_file, save_file)
        #Path(f"{FILES_DIR}{file_name}").rename(save_file)
        keys_string = ', '.join(insert_dict.keys())
        values_string = ', '.join(['%s'] * len(insert_dict))
        insert_query = f"INSERT INTO specials ({keys_string}) VALUES ({values_string});"
        cur.execute(insert_query, tuple(insert_dict.values()))
        con.commit()
con.close()

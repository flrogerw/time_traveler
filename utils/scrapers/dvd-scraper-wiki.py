import csv
import hashlib
import re
import shutil

import psycopg2
import urllib.parse
import os
from urllib.parse import urlparse
from pathlib import Path
from urllib.request import urlretrieve
import sqlite3
import requests
from bs4 import BeautifulSoup
import subprocess
import datetime
from nltk.corpus import stopwords
import string
import glob

con = psycopg2.connect(database="time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.201", port=5432)

cur = con.cursor()
stop = set(stopwords.words('english') + list(string.punctuation))
translator = str.maketrans('', '', string.punctuation)
episodes = {}

BLACKOUT_DURATION = 1.8

DEV_MODE = False
SHOW_NAME = 'Scooby_Doo_Show'
SHOW_ID = 197
FILES_DIR = "/Volumes/TTBS/dump/scooby_doo_show"
URL = "https://en.wikipedia.org/wiki/List_of_The_Scooby-Doo_Show_episodes"

page = requests.get(URL)
soup = BeautifulSoup(page.content, "html.parser")
table = soup.find_all("table", class_="wikitable")
rows = table[2].find_all("tr")

# description_rows.pop(0)
#description_rows = description_rows[:-51]
rows.pop(0)


# del rows[50:-130]
#rows = rows[:-51]
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
    logfile = os.path.join('./', "FFMPEGLOG.txt")
    subprocess.call(
        f'ffmpeg -i {filename} -vf "blackdetect=d={BLACKOUT_DURATION}:pix_th=0.05" -an -f null - 2>&1 | grep blackdetect > {logfile}',
        shell=True)
    logfile = logfile.replace("\ ", " ")
    with open(logfile, 'r') as log_file:
        row = log_file.readline()
        while row != '':  # The EOF char is an empty string
            row = log_file.readline()
            if 'black_start' in row:
                deltas = row.split("\n")[0:1][0].split(' ')[3:]
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


def get_date(airdate):
    # For 'Spring-1962' pattern
    # seasons = {'spring': 4, 'summer': 7, 'fall': 9, 'winter': 1}
    # month = seasons[''.join(filter(str.isalpha, airdate)).lower()]
    # airdate = f"{''.join(filter(str.isdigit, airdate))}-{month}-1"

    # Pass through for ISO format (1962-04-21)
    return datetime.datetime.strptime(airdate.strip(), "%B %d, %Y")


def get_file_name(url_string):
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    filename = str(file.with_suffix('')).translate(translator).lower().strip()
    filename = filename.replace(' ', '')
    return filename


def main():
    season = 1
    for i, row in enumerate(rows):
        #if not row.find_all('th')[0].text.isnumeric():
        tds = row.find_all('td')
        episode_no = int(tds[0].text.split('-')[1])
        #episode_no = int(row.find_all('th')[0].text)
        if episode_no == 17: season += 1
        if episode_no >= 17: episode_no = episode_no - 16

        airdate = tds[3].text
        d = get_date(airdate)
        year = int(str(d.year)[2:])
        key = tds[1].text.replace(' ', '').translate(translator)

        episodes[key] = {
            "season": season,
            "episode": episode_no,
            "title": tds[1].text.encode('ascii', 'ignore').decode('ascii').replace('"', '').strip(),
            "description": tds[4].text.encode('ascii', 'ignore').decode('ascii').replace('"', '').strip(),
            "airdate": d.date().strftime('%Y-%m-%d'),
            "year": year,
            "decade": f"{year - (year % 10)}s"
        }


    for file in glob.glob(f"{FILES_DIR}/*.mp4"):
        failed = []
        url_string = urllib.parse.unquote(file)
        filename = get_file_name(url_string)
        for e in episodes:
            token = episodes[e]['title'].lower().translate(translator).replace(' ', '')
            if token in filename and len(episodes[e]['title']) > 2:
                breaks = []
                e = episodes[e]
                save_file = f"{SHOW_NAME}_{e['title'].translate(translator).replace(' ', '_')}.mp4"
                save_path_str = f"/Volumes/TTBS/time_traveler/{e['decade']}/{e['year']}/{save_file}"
                print(save_path_str)
                if not DEV_MODE:
                    processed_file = process_remove_bars(file)
                    processed_file_path = Path(processed_file)
                    save_path = Path(save_path_str)
                    shutil.copy(processed_file_path, save_path)
                    os.remove(processed_file_path)

                insert_dict = {
                    "episode_file": save_file,
                    "show_id": SHOW_ID,
                    "episode_title": e['title'],
                    "episode_description": e['description'].replace('"', '\"'),
                    "show_season_number": e['season'],
                    "episode_number": e['episode'],
                    "episode_airdate": e['airdate'],
                    "start_point": 0,
                    "end_point": int(get_length(save_path_str)) if not DEV_MODE else 0
                }

                if DEV_MODE:
                    print(insert_dict)
                else:
                    keys_string = ', '.join(insert_dict.keys())
                    values_string = ', '.join(['%s'] * len(insert_dict))
                    insert_query = f"INSERT INTO episodes ({keys_string}) VALUES ({values_string}) RETURNING episode_id;"
                    cur.execute(insert_query, tuple(insert_dict.values()))
                    row_id = cur.fetchone()[0]
                    con.commit()
                    for times in get_blackout(save_path):
                        if int(times[0]) == 0:
                            x = 1
                            # cur.execute(f"UPDATE episodes SET episode_start_point = {int(times[1])} WHERE episode_id = {row_id}")
                        else:
                            cur.execute(
                                f"INSERT INTO commercial_breaks VALUES {(row_id, int(times[0]), int(times[1]))}")
                            breaks.append((row_id, int(times[0]), int(times[1])))
                    #cur.executemany("INSERT INTO commercial_breaks VALUES(?, ?, ?)", breaks)
                    con.commit()
                    print(f"INSERTED {e['title']}")
            else:
                failed.append(filename)
    cur.close()
    con.close()
    print(str(set(failed)))


if __name__ == '__main__':
    main()

import csv
import re
import urllib.parse
import os
from urllib.parse import urlparse
from pathlib import Path
import urllib.request
import sqlite3
import subprocess
import datetime
import nltk
import psycopg2
import requests
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
import string
import json
from html import unescape
import time

con = psycopg2.connect(database="Time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.149", port=5432)

cur = con.cursor()
stop = set(stopwords.words('english') + list(string.punctuation))
translator = str.maketrans('', '', string.punctuation)
episodes = {}

BLACKOUT_DURATION = 1.8

IS_DEV_MODE = False
SHOW_NAME = 'CHIPS'
SHOW_ID = 124
URL_CSV = "/Users/roger.williams/Desktop/file_to_do/chips.csv"
IMDB_URL = 'https://www.imdb.com/title/tt0075488/episodes/?season=1'


def get_blackout(filename):
    result_times = []
    logfile = os.path.join('./', "FFMPEGLOG.txt")
    subprocess.call(
        f'ffmpeg -i {filename} -vf "blackdetect=d={BLACKOUT_DURATION}:pix_th=0.10" -an -f null - 2>&1 | grep blackdetect > {logfile}',
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
    return datetime.datetime.strptime(airdate, '%Y-%m-%d')


def get_file_name(url_string):
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    filename = str(file.with_suffix('')).translate(translator).lower().strip()
    filename = filename.replace(' ', '')
    return filename


def get_episode_season(url_string):
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    date_str = ' '.join(str(file.with_suffix('')).split('_')[-3:])
    # file_name = re.sub(r'^COLUMBO', '', str(file.with_suffix('')), flags=re.IGNORECASE).split('.')[1]
    file_name = str(file.with_suffix('')).split('_')[6]
    #print(file_name)
    s = int(file_name[1:3])
    e = int(file_name[-2:])
    #print(s, e)
    return s, e


def get_imdb_episodes():
    headers = {'User-Agent': 'Mozilla/5.0'}
    page = requests.get(IMDB_URL, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    rows = soup.find_all("script", type='application/json')
    data = json.loads(rows[0].text)
    return data['props']['pageProps']['contentData']['section']['episodes']


def main():
    rows = get_imdb_episodes()
    for i, row in enumerate(rows['items']):
        episode_no = row['episode']
        season = row['season']
        title = unescape(row['titleText'])
        airdate = f"{row['releaseDate']['year']}-{row['releaseDate']['month']}-{row['releaseDate']['day']}"
        d = get_date(airdate)
        year = int(str(d.year)[2:])
        key = title.replace(' ', '').translate(translator)
        description = unescape(row['plot'].strip())

        episodes[key] = {
            "season": season,
            "episode": episode_no,
            "title": title.replace('\xa0', ' '),
            "description": description.replace('\xa0', ' '),
            "airdate": d.date(),
            "year": year,
            "decade": f"{year - (year % 10)}s"
        }
        continue
    with open(URL_CSV, newline='') as csvfile:
        cvs_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        failed = []
        for row in cvs_reader:
            link = f"https://ia600301.us.archive.org/14/items/{row[0].replace('/download','')}"
            url_string = urllib.parse.unquote(link)
            filename = get_file_name(url_string)
            ep_season, ep_no = get_episode_season(url_string)

            for e in episodes:
                token = episodes[e]['title'].lower().translate(translator).replace(' ', '')
                #if token in filename and len(episodes[e]['title']) > 2:
                if int(episodes[e]['episode']) == ep_no and  int(episodes[e]['season']) == ep_season and len(episodes[e]['title']) > 2:
                    breaks = []
                    e = episodes[e]
                    print(e)
                    save_file = f"{SHOW_NAME}_{e['title'].translate(translator).replace(' ', '_')}.mp4"
                    save_path = f"/Volumes/shared/time_traveler/{e['decade']}/{e['year']}/{save_file}"
                    print(save_path)
                    if not IS_DEV_MODE:
                        opener = urllib.request.build_opener()
                        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
                        urllib.request.install_opener(opener)
                        urllib.request.urlretrieve(link, save_path)
                    insert_dict = {
                        "episode_file": save_file,
                        "show_id": SHOW_ID,
                        "episode_title": e['title'],
                        "episode_description": e['description'],
                        "show_season_number": e['season'],
                        "episode_number": e['episode'],
                        "episode_airdate": e['airdate'],
                        "episode_start_point": 0,
                        "episode_end_point": int(get_length(save_path)) if not IS_DEV_MODE else 0
                    }

                    if IS_DEV_MODE:
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
                        # cur.executemany("INSERT INTO commercial_breaks VALUES(?, ?, ?)", breaks)
                        con.commit()
                        print(f"INSERTED {e['title']}")
                        # time.sleep(120)

                else:
                    failed.append((token, filename))
        con.close()
        print(str(set(failed)))


if __name__ == '__main__':
    main()

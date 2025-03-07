import csv
import re
import urllib.parse
import os
from urllib.parse import urlparse
from pathlib import Path
import psycopg2
import requests
from bs4 import BeautifulSoup
import subprocess
import datetime
from nltk.corpus import stopwords
import string
import glob
import json
from html import unescape

con = psycopg2.connect(database="Time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.149", port=5432)

cur = con.cursor()
stop = set(stopwords.words('english') + list(string.punctuation))
translator = str.maketrans('', '', string.punctuation)
episodes = {}

BLACKOUT_DURATION = 1.8

DEV_MODE = True
SHOW_NAME = 'Twilight_Zone'.replace(' ', '_')
SHOW_ID = 127
FILES_DIR = "/Volumes/shared/dump/the-twilight-zone-1959-s-01-e-00-original-pilot"
IMDB_URL = 'https://www.imdb.com/title/tt0052520/episodes/?season=1'


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


def get_date(release_date, last_year):
    # For 'Spring-1962' pattern
    # seasons = {'spring': 4, 'summer': 7, 'fall': 9, 'winter': 1}
    # month = seasons[''.join(filter(str.isalpha, airdate)).lower()]
    # airdate = f"{''.join(filter(str.isdigit, airdate))}-{month}-1"
    release_date = release_date if release_date is not None else {}
    last_year = release_date['year'] if 'year' in release_date and release_date['year'] is not None else last_year
    release_date = release_date if release_date is not None else {}
    day = release_date['day'] if 'day' in release_date and release_date['day'] is not None else '01'
    month = release_date['month'] if 'month' in release_date and release_date['month'] is not None else '01'
    year = release_date['year'] if 'year' in release_date and release_date['year'] is not None else last_year
    return datetime.datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d'), last_year


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
    #file_name = re.sub(r'^Banacek.', '', str(file.with_suffix(''))).split()[0]
    file_name = str(file.with_suffix('')).split()[4]
    #print(file_name, flush=True)
    e = int(file_name[-2:])
    s = int(file_name[1:3])
    print(s, e)
    return s, e


# USED TO PARSE FILENAME FOR MATCHING
def get_raw_filename(url_string):
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    file_name = re.sub(r' SDTV', '', str(file.with_suffix('')))
    return re.sub(r'^Wacky Races - S\d+E\d+ - ', '', file_name)


# USED TO GET SEASON AND EPISODE NUMBER FOR MATCHING
def get_imdb_episodes():
    headers = {'User-Agent': 'Mozilla/5.0'}
    page = requests.get(IMDB_URL, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    rows = soup.find_all("script", type='application/json')
    data = json.loads(rows[0].text)
    return data['props']['pageProps']['contentData']['section']['episodes']


def main():
    last_year = '1900'
    season = 0
    rows = get_imdb_episodes()
    for i, row in enumerate(rows['items']):
        episode_no = row['episode']
        season = row['season']
        title = unescape(row['titleText'])

        d, last_year = get_date(row['releaseDate'], last_year)
        year = int(str(d.year)[2:])
        key = title.replace(' ', '').translate(translator)
        description = unescape(row['plot'].strip()) if 'plot' in row else 'None'

        episodes[key] = {
            "season": season,
            "episode": episode_no,
            "title": title.replace('\xa0', ' '),
            "description": description.replace('\xa0', ' '),
            "airdate": d.date(),
            "year": year,
            "decade": f"{year - (year % 10)}s"
        }

    for file in glob.glob(f"{FILES_DIR}/*.mp4"):
        failed = []
        url_string = urllib.parse.unquote(file)
        filename = get_file_name(url_string)
        # filename = re.sub(r'^thepinkpantherin', '', filename)
        ep_season, ep_no = get_episode_season(url_string)
        # tmp_title = get_raw_filename(url_string)
        #continue
        for e in episodes:
            token = episodes[e]['title'].lower().translate(translator).replace(' ', '')
            #if token in filename and len(episodes[e]['title']) > 2:
                #if tmp_title.lower().translate(translator).replace(' ', '') in token and len(episodes[e]['title']) > 2:
                #if int(episodes[e]['episode']) == int(tmp_title) and len(episodes[e]['title']) > 2:
            if int(episodes[e]['episode']) == int(ep_no) and  int(episodes[e]['season']) == int(ep_season) and len(episodes[e]['title']) > 2:
                # if tmp_title.date() == episodes[e]['airdate'] and len(episodes[e]['title']) > 2:
                breaks = []
                e = episodes[e]
                save_file = f"{SHOW_NAME}_{e['title'].translate(translator).replace(' ', '_')}.mp4"
                #save_file = f"{SHOW_NAME}_S{ep_season}_E{ep_no}.mp4"
                #save_file = f"{e['title'].translate(translator).replace(' ', '_')}.mp4"
                #save_file = f"{SHOW_NAME}_{tmp_title.translate(translator).replace(' ', '_')}.mp4"
                save_path = f"/Volumes/shared/time_traveler/{e['decade']}/{e['year']}/{save_file}"
                print(save_path)
                if not DEV_MODE:
                    Path(file).rename(save_path)
                insert_dict = {
                    "episode_file": save_file,
                    "show_id": SHOW_ID,
                    "episode_title": e['title'],
                    "episode_description": e['description'].replace('"', '\"'),
                    "show_season_number": e['season'],
                    "episode_number": e['episode'],
                    "episode_airdate": e['airdate'],
                    "episode_start_point": 0,
                    "episode_end_point": int(get_length(save_path)) if not DEV_MODE else 0
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
                    # cur.executemany("INSERT INTO commercial_breaks VALUES(?, ?, ?)", breaks)
                    con.commit()
                    print(f"INSERTED {e['title']}")
            else:
                failed.append(filename)
    cur.close()
    con.close()
    print(str(set(failed)))


if __name__ == '__main__':
    main()

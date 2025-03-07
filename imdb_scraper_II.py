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
import hashlib
from multiprocessing import Pool, cpu_count
import shutil

con = psycopg2.connect(database="time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.201", port=5432)
cur = con.cursor()

stop = set(stopwords.words('english') + list(string.punctuation))
translator = str.maketrans('', '', string.punctuation)

BLACKOUT_DURATION = 1.8
DEV_MODE = False
SHOW_NAME = 'Flying_Nun'.replace(' ', '_')
SHOW_ID = 210
FILES_DIR = "/Volumes/TTBS/dump/flying_nun"
IMDB_URL = 'https://www.imdb.com/title/tt0061252/episodes/?season=2'


def get_episode_season(url_string):
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    file_name = str(file.with_suffix('')) #.split('-')[0]

    match = re.search(r"S(\d{1,2})E(\d{1,2})", file_name, re.IGNORECASE)
    if match:
        season = int(match.group(1))
        episode = int(match.group(2))
        return season, episode
    return None, None



def process_no_remove_bars(input_file):
    a = urlparse(input_file)
    file = Path(os.path.basename(a.path))
    filename = str(file.with_suffix(''))
    parent_path = Path(input_file).parent

    output_file = f"{parent_path}/{filename}.build.mp4"
    process_cmd = [
        'ffmpeg', '-y', '-loglevel', 'quiet', '-i', input_file,
        '-r', '30',
        '-vf', 'scale=640:480,setdar=4/3',
        '-af', 'loudnorm=I=-26:TP=-2:LRA=7',
        '-b:v', '500k',
        '-c:v', 'h264_videotoolbox',
        '-c:a', 'aac',
        '-b:a', '128k',
        output_file
    ]
    subprocess.run(process_cmd)
    return output_file


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
        f'ffmpeg -i {filename} -vf "blackdetect=d={BLACKOUT_DURATION}:pix_th=0.05" -an -f null - 2>&1 | grep blackdetect > {logfile}',
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


def get_date(release_date, last_year):
    if release_date['day'] is None:
        release_date['day'] = 19
    if release_date['month'] is None:
        release_date['month'] = 12
    year = release_date.get('year', last_year)
    day = release_date.get('day', '01')
    month = release_date.get('month', '01')
    return datetime.datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d'), year


def get_file_name(url_string):
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    filename = str(file.with_suffix('')).translate(translator).lower().strip()
    return filename.replace(' ', '')


#  When so many episodes it has a button for more
def get_imdb_episodes2():
    with open('utils/<HTML FILE>', 'r') as f:
        text = f.read()
        return_rows = {'items': []}
        soup = BeautifulSoup(text, "html.parser")
        rows = soup.find_all("article")
        for row in rows:
            release_date = row.find('span', class_="sc-ccd6e31b-10 dYquTu").text
            date_obj = datetime.datetime.strptime(release_date, '%a, %b %d, %Y')
            date_dict = {
                "month": date_obj.month,
                "day": date_obj.day,
                "year": date_obj.year
            }
            season_episode, title = row.find('div', class_="ipc-title__text").text.split('∙')
            season, episode = season_episode.split('.')
            append_row = {
                'releaseDate': date_dict,
                'plot': re.sub(r'\s+', ' ',
                               row.find('div', class_="ipc-html-content-inner-div").text.replace('\n', '')),
                'titleText': title.strip(),
                'season': int(season.strip()[1:]),
                'episode': int(episode.strip().replace('E', ''))
            }
            return_rows['items'].append(append_row)
        return return_rows


def get_imdb_episodes():
    headers = {'User-Agent': 'Mozilla/5.0'}
    page = requests.get(IMDB_URL, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    rows = soup.find_all("script", type='application/json')
    data = json.loads(rows[0].text)
    return data['props']['pageProps']['contentData']['section']['episodes']


def process_file(file, episodes):
    try:
        url_string = urllib.parse.unquote(file)
        ep_season, ep_no = get_episode_season(url_string)

        for e in episodes:
            if int(episodes[e]['episode']) == int(ep_no) and int(episodes[e]['season']) == int(ep_season) and len(
                    episodes[e]['title']) > 2:
                e = episodes[e]
                save_file = f"{SHOW_NAME}_{e['title'].translate(translator).replace(' ', '_')}.mp4"
                save_path_str = f"/Volumes/TTBS/time_traveler/{e['decade']}/{e['year']}/{save_file}"
                print(save_path_str)
                if not DEV_MODE:
                    processed_file = process_remove_bars(file)
                    if processed_file:
                        processed_file_path = Path(processed_file)
                        save_path = Path(save_path_str)
                        shutil.copy(processed_file_path, save_path)
                        os.remove(processed_file_path)
                        episode_length = int(get_length(save_path))
                        breaks = get_blackout(save_path)
                        save_episode_to_db(e, save_file, episode_length, breaks)
                    else:
                        print(f"Crop values could not be detected for {file}")
                else:
                    print(e)
    except Exception as ex:
        print(f"Failed to process {file}: {str(ex)}")


def save_episode_to_db(e, save_file, episode_length, breaks):
    insert_dict = {
        "episode_file": save_file,
        "show_id": SHOW_ID,
        "episode_title": e['title'],
        "episode_description": e['description'].replace('"', '\"'),
        "show_season_number": e['season'],
        "episode_number": e['episode'],
        "episode_airdate": e['airdate'],
        "start_point": 0,
        "end_point": episode_length if not DEV_MODE else 0
    }

    keys_string = ', '.join(insert_dict.keys())
    values_string = ', '.join(['%s'] * len(insert_dict))
    insert_query = f"INSERT INTO episodes ({keys_string}) VALUES ({values_string}) RETURNING episode_id;"
    cur.execute(insert_query, tuple(insert_dict.values()))
    row_id = cur.fetchone()[0]
    con.commit()

    durations_dict = {
        "episode_id": row_id,
        "start_point": 0,
        "end_point": episode_length if not DEV_MODE else 0
    }
    keys_string = ', '.join(durations_dict.keys())
    values_string = ', '.join(['%s'] * len(durations_dict))
    durations_insert_query = f"INSERT INTO episode_durations ({keys_string}) VALUES ({values_string});"
    cur.execute(durations_insert_query, tuple(durations_dict.values()))

    for times in breaks:
        cur.execute(f"INSERT INTO commercial_breaks VALUES {(row_id, int(times[0]), int(times[1]))}")
    con.commit()


def main():
    last_year = '1900'
    episodes = {}
    rows = get_imdb_episodes()

    # rows['items'].pop(0)

    for i, row in enumerate(rows['items']):
        episode_no = row['episode']
        season = row['season']
        title = unescape(row['titleText'])
        d, last_year = get_date(row['releaseDate'], last_year)
        year = int(str(d.year)[2:])
        key = title.replace(' ', '').translate(translator)
        if 'plot' in row:
            raw_description = BeautifulSoup(unescape(row['plot'].strip()), features="html.parser")
        description = raw_description.get_text() if 'plot' in row else 'None'

        episodes[key] = {
            "season": season,
            "episode": episode_no,
            "title": title.replace('\xa0', ' '),
            "description": description.replace('\xa0', ' '),
            "airdate": d.date(),
            "year": f"{year:02}",
            "decade": f"{(year // 10) % 10}0s"
        }

    files = glob.glob(f"{FILES_DIR}/*.mp4")
    with Pool(4) as p:
        p.starmap(process_file, [(file, episodes) for file in files])

    cur.close()
    con.close()


if __name__ == '__main__':
    main()

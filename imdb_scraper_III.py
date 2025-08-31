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
from multiprocessing import Pool
import shutil
import random
import threading
import time

con = psycopg2.connect(database="time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.201", port=5432)
cur = con.cursor()

stop = set(stopwords.words('english') + list(string.punctuation))
translator = str.maketrans('', '', string.punctuation)

BLACKOUT_DURATION = 1.8
#DEV_MODE = False
DEV_MODE = True
SHOW_NAME = 'In_Search_Of'.replace(' ', '_')
SHOW_ID = 214
FILES_DIR = "/Volumes/TTBS/dump/adam-12"
IMDB_URL = 'https://www.imdb.com/title/tt0090473/episodes/?season=1'


def run_ffmpeg_blackdetect(video_path):
    """Extract black screen segments using ffmpeg."""
    cmd = [
        "ffmpeg", "-i", video_path,
        "-vf", "blackdetect=d=1:pix_th=0.10",  # 1 second black min, 10% pixel threshold
        "-an", "-f", "null", "-"
    ]
    result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
    black_segments = []

    for line in result.stderr.splitlines():
        match = re.search(r"black_start:(\d+\.?\d*)\s+black_end:(\d+\.?\d*)", line)
        if match:
            black_segments.append((float(match.group(1)), float(match.group(2))))
    return black_segments


def run_ffmpeg_silencedetect(video_path):
    """Extract silence segments using ffmpeg."""
    cmd = [
        "ffmpeg", "-i", video_path,
        "-af", "silencedetect=n=-50dB:d=1",  # silence = below -50dB for 1 second
        "-f", "null", "-"
    ]
    result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
    silence_segments = []
    start = None

    for line in result.stderr.splitlines():
        if "silence_start" in line:
            start = float(line.split("silence_start: ")[1])
        elif "silence_end" in line and start is not None:
            end = float(line.split("silence_end: ")[1].split()[0])
            silence_segments.append((start, end))
            start = None
    return silence_segments


def merge_segments(black, silence, tolerance=1.0):
    """Find overlapping or close black+silence segments."""
    commercials = []
    for b_start, b_end in black:
        for s_start, s_end in silence:
            latest_start = max(b_start, s_start)
            earliest_end = min(b_end, s_end)
            overlap = earliest_end - latest_start

            if overlap >= 0 or abs(b_end - s_start) <= tolerance or abs(s_end - b_start) <= tolerance:
                commercials.append((min(b_start, s_start), max(b_end, s_end)))
    return merge_close_segments(commercials)


def merge_close_segments(segments, max_gap=1.0):
    """Merge segments that are close together."""
    if not segments:
        return []

    segments.sort()
    merged = [segments[0]]

    for current in segments[1:]:
        last = merged[-1]
        if current[0] - last[1] <= max_gap:
            merged[-1] = (last[0], max(last[1], current[1]))
        else:
            merged.append(current)
    return merged


def format_time(seconds):
    """Convert seconds to HH:MM:SS.sss format."""
    return f"{int(seconds // 3600):02}:{int((seconds % 3600) // 60):02}:{seconds % 60:06.3f}"


def detect_commercials(video_path: str) -> list:
    print(f"Analyzing: {video_path}")
    black = run_ffmpeg_blackdetect(video_path)
    silence = run_ffmpeg_silencedetect(video_path)
    candidates = merge_segments(black, silence)

    print("\n=== Suspected Commercial Breaks ===")
    breaks = []
    for i, (start, end) in enumerate(candidates):
        breaks.append((round(start,2), round(end, 2)))
        print(f"Segment {i+1}: {format_time(start)} --> {format_time(end)} ({end - start:.2f} seconds)")
    return breaks


def keep_ttbs_alive(interval=5):
    """Runs 'ls' on /Volumes/TTBS every `interval` seconds to keep the mount active."""
    while True:
        try:
            subprocess.run(['ls', '/Volumes/TTBS'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"[WARN] TTBS keep-alive failed: {e}")
        time.sleep(interval)

def get_episode_season(url_string):
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    file_name = str(file.with_suffix('')) #.split('-')[0]

    match = re.search(r"S(\d{1,2})E(\d{1,2})*", file_name, re.IGNORECASE)
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

def random_string(length=10):
    characters = string.ascii_letters + string.digits  # A-Z, a-z, 0-9
    return ''.join(random.choice(characters) for _ in range(length))


def get_blackout(filename):
    result_times = []
    logfile = f"{random_string()}.txt"
    try:
        ret = subprocess.call(
            f'ffmpeg -i "{filename}" -vf "blackdetect=d={BLACKOUT_DURATION}:pix_th=0.05" -an -f null - 2>&1 | grep blackdetect > "{logfile}"',
            shell=True)

        if not os.path.exists(logfile):
            print(f"[WARN] Logfile {logfile} not created")
            return result_times

        with open(logfile, 'r') as log_file:
            for row in log_file:
                if 'black_start' in row:
                    deltas = row.split()[3:]
                    start = float(deltas[0].split(':')[1])
                    end = float(deltas[1].split(':')[1])
                    result_times.append((start, end))
    except Exception as ex:
        print(f"[ERROR] While getting blackout: {ex}")
    finally:
        if os.path.exists(logfile):
            os.remove(logfile)
    return result_times

def get_length(filename):
    result = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries",
                             "format=duration", "-of",
                             "default=noprint_wrappers=1:nokey=1", filename],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    return float(result.stdout)


def get_date(release_date, last_year):
    if release_date is None:
        release_date = {'month': 12, 'day': 10, 'year': 1979, '__typename': 'ReleaseDate'}
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
                save_path = Path(save_path_str)
                print(save_path_str)
                if not DEV_MODE:
                    """
                    processed_file = process_remove_bars(file)
                    if processed_file:
                        processed_file_path = Path(processed_file)
                        
                        if save_path.exists():
                            save_path.unlink()
                        shutil.copy2(processed_file_path, save_path)
                        os.remove(processed_file_path)
                        """
                    episode_length = int(get_length(save_path))
                    breaks = detect_commercials(save_path_str)
                    save_episode_to_db(e, save_file, episode_length, breaks)
                    #else:
                      #  print(f"Crop values could not be detected for {file}")
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

    # Start keep-alive thread
    keep_alive_thread = threading.Thread(target=keep_ttbs_alive, daemon=True)
    keep_alive_thread.start()

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
    with Pool(1) as p:
        p.starmap(process_file, [(file, episodes) for file in files])

    cur.close()
    con.close()


if __name__ == '__main__':
    main()

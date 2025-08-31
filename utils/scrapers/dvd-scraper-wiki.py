import csv
import hashlib
import random
import re
import shutil
import threading
import time
from pprint import pprint

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

BLACKOUT_DURATION = 1.25
SEASON = 1
DEV_MODE = False
SHOW_NAME = 'Baywatch'
SHOW_ID = 249
FILES_DIR = "/Volumes/TTBS/dump/baywatch"
URL = "https://en.wikipedia.org/wiki/List_of_Baywatch_episodes"

page = requests.get(URL)
soup = BeautifulSoup(page.content, "html.parser")
table = soup.find_all("table", class_="wikitable")
rows = table[SEASON].find_all("tr")

for index in sorted([0], reverse=True):
    rows.pop(index)

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



# del rows[50:-130]
# rows = rows[:-51]
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
            '-b:v', '600k',
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


def get_date(airdate):
    # For 'Spring-1962' pattern
    # seasons = {'spring': 4, 'summer': 7, 'fall': 9, 'winter': 1}
    # month = seasons[''.join(filter(str.isalpha, airdate)).lower()]
    # airdate = f"{''.join(filter(str.isdigit, airdate))}-{month}-1"

    # Pass through for ISO format (1962-04-21)
    return datetime.datetime.strptime(airdate.strip(), "%Y-%m-%d")


def get_file_name(url_string):
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    filename = str(file.with_suffix('')).translate(translator).lower().strip()
    filename = filename.replace(' ', '')
    return filename


def main():
    # Start keep-alive thread
    keep_alive_thread = threading.Thread(target=keep_ttbs_alive, daemon=True)
    keep_alive_thread.start()

    for i, row in enumerate(rows):
        classes = row.get("class") or []
        if 'vevent' in classes:
            # if not row.find_all('th')[0].text.isnumeric():
            tds = row.find_all('td')
            divs = rows[i + 1].find_all('div')
            episode_no = 0#int(tds[0].text)
            # episode_no = int(tds[0].text.split('-')[1])
            # episode_no = int(row.find_all('th')[0].text)

            # if episode_no == 17: season += 1
            # if episode_no >= 17: episode_no = episode_no - 16
            airdate = '1989-04-29'#tds[4].find_all('span')[0].text.strip().strip("()")
            d = get_date(airdate)
            year = int(str(d.year)[2:])
            #key = f"S{SEASON:02} E{episode_no:02}"
            key = f"s{SEASON}e{episode_no:02}"

        if 'expand-child' in classes:
            episodes[key] = {
                "season": SEASON,
                "episode": episode_no,
                "title": f"Panic at Malibu Pier",
                # "title": tds[1].text.encode('ascii', 'ignore').decode('ascii').replace('"', '').strip(),
                "description": divs[0].text.encode('ascii', 'ignore').decode('ascii').replace('"', '').strip(),
                # "description": (". ").join([li.get_text(strip=True) for li in row.find_all("li")[:3]]),
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
            if e in filename:
                # if token in filename and len(episodes[e]['title']) > 2:
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
                    for times in detect_commercials(save_path):
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

def keep_ttbs_alive(interval=5):
    """Runs 'ls' on /Volumes/TTBS every `interval` seconds to keep the mount active."""
    while True:
        try:
            subprocess.run(['ls', '/Volumes/TTBS'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"[WARN] TTBS keep-alive failed: {e}")
        time.sleep(interval)

if __name__ == '__main__':
    main()

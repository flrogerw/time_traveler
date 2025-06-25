import csv
import re
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

con = sqlite3.connect("/Volumes/shared/time_traveler/time_traveler.db")
cur = con.cursor()
stop = set(stopwords.words('english') + list(string.punctuation))
translator = str.maketrans('', '', string.punctuation)
episodes = {}

BLACKOUT_DURATION = 1.8

IS_DEV_MODE = True
SHOW_NAME = '12_Oclock_High'
SHOW_ID = 29
URL = "https://en.wikipedia.org/wiki/List_of_The_Scooby-Doo_Show_episodes"
URL_CSV = "/Users/rwilliams1/Desktop/have_gun_will_travel_urls.csv"

page = requests.get(URL)
soup = BeautifulSoup(page.content, "html.parser")
rows = soup.find_all("tr", class_="module-episode-list-row")
description_rows = soup.find_all("tr", class_="expand-child")


# description_rows.pop(0)
# del description_rows[-27:]
# rows.pop(44)
# del rows[50:-130]
#rows = rows[:-26]

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


def main():
    season = 0
    for i, row in enumerate(rows):
        if not row.find_all('th')[0].text.isnumeric():
        #if not row.find_all('td')[1].text.isnumeric():
            continue
        #episode_no = row.find_all('td')[0].text
        episode_no = row.find_all('th')[0].text
        if int(episode_no) == 1: season += 1
        if row.find("td", class_="summary").find("a"):
            title = row.find("td", class_="summary").find("a").get('title')
        else:
            title_text = row.find("td", class_="summary").text
            title = re.findall(r'"([^"]*)"', title_text)[0]

        airdate = row.find("span", class_="bday dtstart published updated itvstart").text
        d = get_date(airdate)
        year = int(str(d.year)[2:])
        key = title.replace(' ', '').translate(translator)
        if len(description_rows) > 0 and description_rows[i]:
            description = description_rows[i].find("div", class_="shortSummaryText").text.split("\n")[1].strip()
        else:
            description = 'None'

        episodes[key] = {
            "season": season,
            "episode": episode_no,
            "title": title.replace('\xa0', ' '),
            "description": description.replace('\xa0', ' '),
            "airdate": d.date(),
            "year": year,
            "decade": f"{year - (year % 10)}s"
        }

    with open(URL_CSV, newline='') as csvfile:
        cvs_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        failed = []
        for row in cvs_reader:
            link = f"https://archive.org{row[0]}"
            url_string = urllib.parse.unquote(link)
            filename = get_file_name(url_string)

            for e in episodes:
                token = episodes[e]['title'].lower().translate(translator).replace(' ', '')
                if token in filename and len(episodes[e]['title']) > 2:
                    breaks = []
                    e = episodes[e]
                    save_file = f"{SHOW_NAME}_{e['title'].translate(translator).replace(' ', '_')}.mp4"
                    save_path = f"/Volumes/shared/time_traveler/{e['decade']}/{e['year']}/{save_file}"
                    if not IS_DEV_MODE:
                        urlretrieve(link, save_path)
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
                        cur.execute(
                            f"INSERT INTO episodes ({','.join(insert_dict)}) VALUES (:episode_file, :show_id,:episode_title,:episode_description,:show_season_number,:episode_number,:episode_airdate,:episode_start_point,:episode_end_point)",
                            insert_dict)
                        con.commit()
                        row_id = cur.lastrowid
                        for times in get_blackout(save_path):
                            if int(times[0]) == 0:
                                x = 1
                                # cur.execute(f"UPDATE episodes SET episode_start_point = {int(times[1])} WHERE episode_id = {row_id}")
                            else:
                                breaks.append((row_id, int(times[0]), int(times[1])))
                        cur.executemany("INSERT INTO commercial_breaks VALUES(?, ?, ?)", breaks)
                        con.commit()
                        print(f"INSERTED {e['title']}")
                else:
                    failed.append(filename)
        con.close()
        print(str(set(failed)))


if __name__ == '__main__':
    main()

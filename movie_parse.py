import psycopg2
import string
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import json
import subprocess
import html

IMDB_URL = 'https://www.imdb.com/title/'
FILES_DIR = '/Volumes/TTBS/dump/raw_movies/'
DEV_MODE = False

translator = str.maketrans('', '', string.punctuation)
con = psycopg2.connect(database="time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.201", port=5432)
cur = con.cursor()

with open('movies.json', 'r') as file:
    movies = [tuple(item) for item in json.load(file)]


# movies = [("The good the bad and the ugly Leone.mp4", "tt0060196", {"name":  "The Good, the Bad and the Ugly"})]


def get_length(filename):
    print('XXXX', filename)
    result = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries",
                             "format=duration", "-of",
                             "default=noprint_wrappers=1:nokey=1", filename],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    return float(result.stdout)


headers = {'User-Agent': 'Mozilla/5.0'}

for movie in movies:
    file_name, imdb_id, extras = movie
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
    movie_end_point = int(get_length(f"{FILES_DIR}{file_name}"))
    save_file = f"/Volumes/TTBS/time_traveler/{decade}/movies/{new_file}.mp4"
    print(save_file)

    insert_dict = {
        'movie_file': f"{new_file}.mp4",
        'movie_name': html.unescape(data['name']),
        'movie_description': html.unescape(data['description']),
        'movie_genre': ', '.join(data['genre']),
        'movie_release_date': release_date,
        'movie_stars': ', '.join([record['name'] for record in data['actor']]),
        'start_point': 0,
        'end_point': movie_end_point,
        'movie_rating': 'G' if data['contentRating'] == 'Approved' else data['contentRating'],
        'imdb_number': imdb_id
    }

    if 'movie_season' in extras:
        insert_dict['movie_season'] = extras['movie_season']

    if DEV_MODE:
        print(insert_dict)
    else:
        Path(f"{FILES_DIR}{file_name}").rename(save_file)
        keys_string = ', '.join(insert_dict.keys())
        values_string = ', '.join(['%s'] * len(insert_dict))
        insert_query = f"INSERT INTO movies ({keys_string}) VALUES ({values_string});"
        cur.execute(insert_query, tuple(insert_dict.values()))
        con.commit()
con.close()

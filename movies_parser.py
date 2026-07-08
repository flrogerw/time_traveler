import psycopg2
import string
from pathlib import Path
import json
import subprocess
import html
import time

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

IMDB_URL = 'https://www.imdb.com/title/'
FILES_DIR = '/Volumes/TTBS/dump/raw_movies/'
DEV_MODE = False

translator = str.maketrans('', '', string.punctuation)

con = psycopg2.connect(
    database="time_traveler",
    user="postgres",
    password="m06Ar14u",
    host="192.168.1.201",
    port=5432
)
cur = con.cursor()

with open('movies.json', 'r') as file:
    movies = [tuple(item) for item in json.load(file)]


def get_length(filename):
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-show_entries",
         "format=duration", "-of",
         "default=noprint_wrappers=1:nokey=1", filename],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    return float(result.stdout)


def fetch_imdb_html(imdb_id, context):
    url = f"{IMDB_URL}{imdb_id}"
    page = context.new_page()

    try:
        page.goto(url, timeout=60000)
        page.wait_for_timeout(3000)

        html_content = page.content()

        # Detect block
        if "403 Forbidden" in html_content or "awswaf" in html_content.lower():
            print(f"[BLOCKED] {imdb_id}")
            return None

        return html_content

    except Exception as e:
        print(f"[ERROR] {imdb_id}: {e}")
        return None

    finally:
        page.close()


with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)

    # 👇 Use saved session if available
    try:
        context = browser.new_context(storage_state="imdb_state.json")
    except:
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36")
        )

    for movie in movies:
        file_name, imdb_id, extras = movie

        print(f"\nProcessing: {imdb_id}")

        html_content = fetch_imdb_html(imdb_id, context)

        if not html_content:
            print(f"[SKIP] {imdb_id}")
            continue

        soup = BeautifulSoup(html_content, "html.parser")

        script = soup.find("script", type="application/ld+json")

        if not script:
            print(f"[NO JSON] {imdb_id}")
            continue

        try:
            data = json.loads(script.string)
        except Exception as e:
            print(f"[JSON ERROR] {imdb_id}: {e}")
            continue

        # ---- Normalize fields ----
        data['genre'] = data.get('genre', [])
        data['description'] = data.get('description', '')
        data['actor'] = data.get('actor', [])

        # ---- Extract metadata ----
        title = data.get("name")
        description = data.get("description")
        rating = data.get("aggregateRating", {}).get("ratingValue")
        rating_count = data.get("aggregateRating", {}).get("ratingCount")
        genres = data.get("genre")
        release_date = data.get("datePublished")
        duration = data.get("duration")

        actors = [actor["name"] for actor in data.get("actor", [])]
        directors = [d["name"] for d in data.get("director", [])]

        print({
            "title": title,
            "rating": rating,
            "genres": genres,
            "release_date": release_date
        })

        # ---- Your existing logic ----
        release_date = extras['movie_release_date'] if 'movie_release_date' in extras else release_date[:4]

        data['contentRating'] = data.get('contentRating', 'Not Rated')
        data['name'] = extras['name'] if 'name' in extras else data['name']

        year = int(str(release_date)[2:])
        decade = f"{str((year - (year % 10))).zfill(2)}s"

        new_file = f"{html.unescape(data['name']).translate(translator)}_{release_date}"
        new_file = new_file.replace(' ', '_')

        movie_path = f"{FILES_DIR}{file_name}"
        movie_end_point = int(get_length(movie_path))

        save_file = f"/Volumes/TTBS/time_traveler/{decade}/movies/{new_file}.mp4"

        movie_rating = 'G' if data['contentRating'] == 'Approved' else data['contentRating']
        movie_rating = extras['movie_rating'] if 'movie_rating' in extras else movie_rating

        insert_dict = {
            'movie_file': f"{new_file}.mp4",
            'movie_name': html.unescape(data['name']),
            'movie_description': html.unescape(data['description']),
            'movie_genre': ', '.join(data['genre']),
            'movie_release_date': release_date,
            'movie_stars': extras.get('movie_stars', ', '.join(actors)),
            'start_point': 0,
            'end_point': movie_end_point,
            'movie_rating': movie_rating,
            'imdb_number': imdb_id
        }

        if 'movie_season' in extras:
            insert_dict['movie_season'] = extras['movie_season']

        if DEV_MODE:
            print(save_file)
            print(insert_dict)
        else:
            Path(movie_path).rename(save_file)

            keys_string = ', '.join(insert_dict.keys())
            values_string = ', '.join(['%s'] * len(insert_dict))
            insert_query = f"INSERT INTO movies ({keys_string}) VALUES ({values_string});"

            cur.execute(insert_query, tuple(insert_dict.values()))
            con.commit()

        # 👇 small delay to avoid getting blocked again
        time.sleep(1)

    # Save session (helps avoid WAF next run)
    context.storage_state(path="imdb_state.json")

    browser.close()

con.close()
import subprocess
import json
from math import gcd
import os
import psycopg2
import logging
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import DictCursor

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection() -> psycopg2.extensions.connection:
    """
    Establish and return a database connection.

    :return: psycopg2 database connection object.
    """
    try:
        conn = psycopg2.connect(
            database=os.getenv('DATABASE'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=5432
        )
        logging.info("Database connection established.")
        return conn
    except Exception:
        logging.exception(f"Failed to connect to the database.")
        raise


def update_db(db_con, movie_name, aspect_ratio):
    cur = db_con.cursor(cursor_factory=DictCursor)  # Cursor to execute queries
    query = sql.SQL("""UPDATE movies SET aspect_ratio = %s WHERE movie_file = %s""")
    # query = sql.SQL("""SELECT * FROM movies WHERE movie_file = %s""")

    # Execute the query and retrieve the next episode
    cur.execute(query, (aspect_ratio, movie_name))
    # db_con.execute(query, (movie_name,))

    db_con.commit()




def get_aspect_ratio(video_file):
    """Retrieve the aspect ratio of a video file using ffprobe."""
    try:
        # Run ffprobe command
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",  # Suppress non-error output
                "-select_streams", "v",  # Select the video stream
                "-show_entries", "stream=width,height",  # Get width and height
                "-of", "json",  # Output in JSON format
                video_file
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # Parse the JSON output
        metadata = json.loads(result.stdout)
        video_stream = metadata['streams'][0]
        width = video_stream['width']
        height = video_stream['height']

        # Calculate and return the aspect ratio
        return f"{width // gcd(width, height)}:{height // gcd(width, height)}"
    except Exception as e:
        print(f"Error retrieving aspect ratio: {e}")
        return None



base_dir = "/Volumes/TTBS/time_traveler"

db = get_db_connection()
for root, dirs, files in os.walk(base_dir):
    # Exclude hidden directories
    dirs[:] = [d for d in dirs if not d.startswith('.')]
    # Check if the current folder is named 'movies'
    if os.path.basename(root).lower() == 'specials':
        for file in files:
            if not file.startswith('.'):
                aspect_ratio = get_aspect_ratio(os.path.join(root, file))
                print(file, aspect_ratio)
                #update_db(db, file, aspect_ratio)

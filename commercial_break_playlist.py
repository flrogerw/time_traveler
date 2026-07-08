import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = os.getenv("DIR_ROOT")

# Database connection settings
db_config = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT"),
}

def get_file_path(airdate, episode_file):
    year = int(airdate.strftime("%y"))
    decade = f"{(year // 10) % 10}0s"
    return f'{ROOT_DIR}/{decade}/{year}/{episode_file}'

def fetch_commercial_breaks(show_id):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    query = """
        SELECT 
            cb.media_id,
            e.episode_file,
            e.episode_airdate,
            cb.break_point,
            cb.resume_point
        FROM commercial_breaks cb
        JOIN episodes e ON cb.media_id = e.episode_id
        WHERE e.show_id = %s
        ORDER BY cb.media_id, cb.break_point ASC;
    """

    cur.execute(query, (show_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def generate_m3u(rows, output_file="playlist.m3u"):
    with open(output_file, "w") as f:
        f.write("#EXTM3U\n")

        for media_id, episode_file, episode_airdate, break_point, resume_point in rows:
            file_path = get_file_path(episode_airdate, episode_file)

            # --- Pre-break clip ---
            pre_start = max(0, break_point - 2.5)  # prevent negative time
            pre_end = break_point + .25

            f.write(f"#EXTVLCOPT:start-time={pre_start:.3f}\n")
            f.write(f"#EXTVLCOPT:stop-time={pre_end:.3f}\n")
            f.write("#EXTVLCOPT:sharpen-sigma=0.5\n")
            f.write(f"{file_path}\n")

            # --- Post-break clip ---
            post_start = resume_point - .25
            post_end = resume_point + 2.5

            f.write(f"#EXTVLCOPT:start-time={post_start:.3f}\n")
            f.write(f"#EXTVLCOPT:stop-time={post_end:.3f}\n")
            f.write("#EXTVLCOPT:sharpen-sigma=0.5\n")
            f.write(f"{file_path}\n")


    print(f"M3U playlist written to {output_file}")

if __name__ == "__main__":
    rows = fetch_commercial_breaks(364)
    generate_m3u(rows)

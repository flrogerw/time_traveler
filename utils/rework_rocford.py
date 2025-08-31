import psycopg2
import os
import shutil
from datetime import datetime

# --- Database connection setup ---
conn = psycopg2.connect(database="time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.201", port=5432)
cur = conn.cursor()

# --- Run the query ---
cur.execute("""
    SELECT * FROM public.episodes
    WHERE show_id = 126
    AND show_season_number = 1
    ORDER BY show_season_number, episode_number
""")
rows = cur.fetchall()
colnames = [desc[0] for desc in cur.description]

# --- Process each row ---
for row in rows:
    db_row = dict(zip(colnames, row))

    airdate_str = db_row['episode_airdate']
    try:
        airdate = datetime.strptime(str(airdate_str), "%Y-%m-%d")
    except ValueError:
        print(f"Skipping invalid date: {airdate_str}")
        continue

    year = airdate.year
    decade = f"{year % 100 // 10}0s"
    year_short = str(year)[-2:]

    source_path = f"/Volumes/TTBS/time_traveler/{decade}/{year_short}/{db_row['episode_file']}"
    target_filename = f"S{int(db_row['show_season_number']):02}E{int(db_row['episode_number']):02}.mp4"
    target_path = f"/Volumes/TTBS/dump/rockford/{target_filename}"

    # Copy if source exists
    if os.path.exists(source_path) and not os.path.exists(target_path):
        try:
            shutil.copy2(source_path, target_path)
            print(f"Copied to {target_path}")
        except FileExistsError:
            print(f"Target already exists: {target_path}")
        except Exception as e:
            print(f"Error copying {source_path} -> {target_path}: {e}")
    else:
        print(f"Source not found: {source_path}")

# --- Cleanup ---
cur.close()
conn.close()
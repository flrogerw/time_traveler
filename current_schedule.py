#!/usr/bin/env python
import logging
import math
import os
import re
from datetime import timedelta, datetime
from itertools import groupby
import random
from pathlib import Path

import inflect
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import DictCursor

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

START_TIME = "08:00"
DIRECTORY = "/Volumes/TTBS/time_traveler/sys/playlists"
DOWNLOAD_DIR = "/Volumes/TTBS/time_traveler/sys/schedules"

p = inflect.engine()


def write_to_file(data: str, filename: str) -> None:
    """Write the provided data to a file in the specified directory with formatted JSON content.

    Args:
        data (str): The data to be written to the file. It is expected to be a string, and it will be formatted as JSON.
        filename (str): The name of the file where the data will be saved. The file is saved in the directory specified by DOWNLOAD_DIR.

    Returns:
        None: This function does not return any value.

    """
    # Open the specified file in write mode and write the formatted JSON data
    with Path.open(Path(f"{DOWNLOAD_DIR}/{filename}"), 'w', newline='') as f:
        f.write(data)


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
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise


def parse_m3u8(file_path, start_time, channel):
    """
    Parse an M3U8 file and calculate cumulative durations with accurate mapping.

    Args:
        file_path (str): Path to the M3U8 file.
        start_time (str): Start time in HH:MM:SS format.
    """
    # Convert start_time to a datetime object
    clock_time = datetime.strptime(start_time, "%H:%M")
    cumulative_duration = timedelta()
    current_start = None
    current_stop = None
    video_path = None

    with open(file_path, 'r') as file:
        lines = file.readlines()
    schedule_list = []
    for line in lines:
        line = line.strip()
        if line.startswith("#EXTVLCOPT:start-time="):
            # Extract start time
            current_start = int(re.search(r"start-time=(\d+)", line).group(1))
        elif line.startswith("#EXTVLCOPT:stop-time="):
            # Extract stop time
            current_stop = int(re.search(r"stop-time=(\d+)", line).group(1))
        elif not line.startswith("#"):
            video_path = line
            # This is a video path; process previous entry before storing the new one
            if video_path and current_start is not None and current_stop is not None:
                # Calculate duration and update times
                duration = timedelta(seconds=(current_stop - current_start))
                wall_start_time = clock_time + cumulative_duration
                cumulative_duration += duration
                duration = duration.seconds / 60
                rounded_minutes = math.ceil(duration / 30) * 30
                mins = ''
                if rounded_minutes >= 120:
                    if rounded_minutes % 60 > 0:
                        mins = f" {rounded_minutes % 60} mins."
                    hours = int(rounded_minutes / 60)
                    word_hours = p.number_to_words(hours)
                    display_duration = f"{word_hours.capitalize()} hours{mins}"
                else:
                    display_duration = f"{rounded_minutes} mins."

                # Print results
                if 'commercial' not in video_path.lower():
                    file_name = os.path.basename(video_path)
                    schedule_list.append({
                        "show": file_name,
                        "duration": rounded_minutes,
                        "display_duration": display_duration,
                        "start": wall_start_time.strftime('%H:%M'),
                        "wall_clock": wall_start_time.strftime('%I:%M'),
                        "channel": channel
                    })

            # Update for the next video
            current_start = None
            current_stop = None

    # Process the last entry
    if video_path and current_start is not None and current_stop is not None:
        duration = timedelta(seconds=(current_stop - current_start))
        wall_start_time = clock_time + cumulative_duration
        cumulative_duration += duration
        duration = duration.seconds / 60
        rounded_minutes = math.ceil(duration / 30) * 30
        mins = ''
        if rounded_minutes >= 120:
            if rounded_minutes % 60 > 0:
                mins = f" {rounded_minutes % 60} mins."
            hours = int(rounded_minutes / 60)
            word_hours = p.number_to_words(hours)
            display_duration = f"{word_hours.capitalize()} hours{mins}"
        else:
            display_duration = f"{rounded_minutes} mins."

        if 'commercial' not in video_path.lower():
            file_name = os.path.basename(video_path)
            schedule_list.append({
                "show": file_name,
                "duration": rounded_minutes,
                "display_duration": display_duration,
                "start": wall_start_time.strftime('%H:%M'),
                "wall_clock": wall_start_time.strftime('%I:%M'),
                "channel": channel
            })

    return schedule_list


def channel_number(file_name):
    """Extracts the first number from a file name."""
    match = re.search(r'\d+', file_name)
    if match:
        return int(match.group())
    return None


def get_m3u_files(search_dir):
    """Reads a directory and returns a list of .m3u files."""
    return [file for file in os.listdir(search_dir) if file.endswith('.m3u')]


def get_sort_key(item):
    start_hour = int(START_TIME[:2])
    sort_hour = int(item['start'][:2])
    adjusted_hour = 24 + sort_hour if sort_hour < start_hour else sort_hour  # Treat midnight as hour 24
    return (adjusted_hour, item['start'])  # Sort by hour, then time


def get_group_key(item):
    group_hour = item['start'][:2]
    return '24' if group_hour == '00' else group_hour  # Group by adjusted hour


def set_playlist_hour(playlist_hour):
    return {playlist_hour: {'shows': []}}


def get_json(grouped_by_hour):
    json = {"start_time": START_TIME, 'time_periods': []}
    for hour, group in grouped_by_hour:
        hour_label = '24:00' if hour == '24' else f"{hour}:00"
        time_slot_json = set_playlist_hour(hour_label)
        for entry in group:
            if int(entry['start'].split(':')[1]) >= 30 and int(hour_label[-2:]) != 30:
                json['time_periods'].append(time_slot_json)
                hour_label = f"{hour}:30"
                time_slot_json = set_playlist_hour(hour_label)
            time_slot_json[hour_label]['shows'].append(entry)
        json['time_periods'].append(time_slot_json)
    return json


def get_db_data(show_list: list, tables_to_search: list) -> list:
    meta_data = []
    db = get_db_connection()
    cur = db.cursor(cursor_factory=DictCursor)

    for t in tables_to_search:
        table_name, column_name = t
        query_str = "SELECT * FROM {table} WHERE {column} IN %s"
        if table_name == 'episodes':
            query_str = """SELECT t.*, s.show_name, s.show_genre FROM {table} t 
                            JOIN shows s ON t.show_id = s.show_id WHERE {column} IN %s"""
        # Construct the query safely using psycopg2.sql
        query = sql.SQL(query_str).format(
            table=sql.Identifier(table_name),
            column=sql.Identifier(column_name),
        )
        # Execute the query and retrieve the next episode
        cur.execute(query, (tuple(show_list),))
        formatted_record = [{**dict(record), "type": table_name} for record in cur.fetchall()]
        meta_data = meta_data + formatted_record
    return meta_data


def populate_meta(playlists: list):
    show_list = [entry['show'] for entry in playlists]
    tables_to_search = [('episodes', 'episode_file'), ('specials', 'specials_file'), ('movies', 'movie_file')]
    records = get_db_data(show_list, tables_to_search)
    return records


def normalize_meta(to_normalize):
    minimized = []
    for show in to_normalize:
        match show["type"]:
            case "episodes":
                specific_data = {
                    "title": show['show_name'].upper(),
                    "description": show['episode_description'],
                    "genre": show['show_genre'].split(',')[0].capitalize(),
                    "stars": show['episode_co_stars'],
                }
            case "specials":
                specific_data = {
                    "title": show['specials_title'].upper(),
                    "description": show['specials_description'],
                    "genre": show['specials_genre'].split(',')[0].capitalize(),
                    "stars": show['specials_stars'],
                }
            case "movies":
                specific_data = {
                    "title": 'MOVIE',
                    "description": f"""\"{show['movie_name']}\" ({show['movie_release_date']}) {show['movie_description']}""",
                    "genre": show['movie_genre'].split(',')[0].capitalize(),
                    "stars": show['movie_stars'],
                }

        common_data = {
            'duration': show['duration'],
            'display_duration': show['display_duration'],
            'start': show['start'],
            'wall_clock': show['wall_clock'],
            "channel": show['channel'],
            "type": show['type'],
        }

        common_data.update(specific_data)
        minimized.append(common_data)

    return minimized


def get_lookup_table(meta_data):
    lookup = {}
    for show in meta_data:
        match show["type"]:
            case "episodes":
                lookup[show["episode_file"]] = show
            case "specials":
                lookup[show["specials_file"]] = show
            case "movies":
                lookup[show["movie_file"]] = show
    return lookup


def merge_meta(playlists, meta_data):
    lookup = get_lookup_table(meta_data)
    merged_list = []

    for show in playlists:
        type_match = lookup.get(show["show"])
        if type_match:
            # Merge the two dicts
            merged_dict = {**show, **type_match}
            merged_list.append(merged_dict)

    return merged_list


def trim_time(time_slot):
    parts = time_slot.split(':')
    parts[0] = parts[0].lstrip('0') or '0'  # Ensure '0' for '00'
    return ':'.join(parts)


def get_html(json: dict):
    time_of_day = ''
    html = f"""<!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <style>
                        body {{
                            font-family: Helvetica; /* Specify the font family */
                            font-size: 16px;              /* Optional: Set the base font size */
                            line-height: 1.2;             /* Optional: Adjust line spacing */
                        }}
                        .description_div {{
                            width: 800px;
                            margin-left: 65px;
                            margin-top: 3px;
                        }}
                        .title_div {{
                            float: left;
                            margin-left: 15px;
                            font-weight: bold;
                        }}
                        .wrapper {{
                            margin-top: 10px;
                        }}
                        .float_left {{
                            float: left;
                            width: 50px;
                            text-align: right;
                            padding-right: 10px;
                        }}
                        .empty_spacer {{
                            width: 60px; /* Set the fixed width */
                            min-width: 60px; /* Ensure it can't shrink below 100px */
                            height: auto; /* Adjust height automatically based on content */
                            display: block; /* Ensure it behaves as a block element */
                            float: left;
                        }}
                        .special_box {{
                            width: 50px;           /* Set the width of the square */
                            height: 14px;          /* Set the height of the square */
                            border: 2px solid black; /* Border around the square */
                            display: flex;          /* Flexbox for centering content */
                            justify-content: center; /* Center horizontally */
                            align-items: center;    /* Center vertically */
                            font-size: 10px;        /* Font size for the digit */
                            font-weight: bold;      /* Make the digit bold */
                            color: black;           /* Color of the digit */
                            float: left;
                            clear: left;
                            margin-top: 3px;
                            margin-left: 65px;
                            margin-right: 5px;
                        
                        }}
                        .border-box {{
                              width: 16px;           /* Set the width of the square */
                              height: 16px;          /* Set the height of the square */
                              border: 2px solid black; /* Border around the square */
                              border-radius: 6px;    /* Rounded corners */
                              display: flex;          /* Flexbox for centering content */
                              justify-content: center; /* Center horizontally */
                              align-items: center;    /* Center vertically */
                              font-size: 15px;        /* Font size for the digit */
                              font-weight: bold;      /* Make the digit bold */
                              color: black;           /* Color of the digit */
                              float: left;
                              margin-left:  5px;
                            }}
                        .black-box {{
                            position: relative;   /* Positioning context for the absolute digit */
                            width: 20px;         /* Width of the box */
                            height: 20px;        /* Height of the box */
                            background-color: black; /* Black background */
                            display: flex;        /* Flexbox to center the digit */
                            justify-content: center;
                            align-items: center;
                            color: white;         /* Color of the digit (white in this case) */
                            font-size: 1rem;      /* Size of the digit */
                            font-weight: bold;    /* Make the digit bold */
                            border-radius: 6px;   /* Optional: rounded corners for the */
                            float: left;
                            margin-left:  5px;
                        }}
                    </style>
                </head>
                <body><h2>{datetime.now().strftime('%A')}</h2>"""

    morning_start = datetime.strptime('00:00:00', '%H:%M:%S').time()
    morning_end = datetime.strptime('11:59:59', '%H:%M:%S').time()
    afternoon_start = datetime.strptime('12:00:00', '%H:%M:%S').time()
    afternoon_end = datetime.strptime('17:59:59', '%H:%M:%S').time()
    evening_start = datetime.strptime('18:00:00', '%H:%M:%S').time()
    evening_end = datetime.strptime('23:59:59', '%H:%M:%S').time()
    non_channels = [3, 6, 8, 10]

    for time_slot in json['time_periods']:
        time_period = next(iter(time_slot.keys()))
        period_to_check = f'00:{time_period[-2:]}' if time_period[:2] == '24' else time_period
        time_to_check = datetime.strptime(period_to_check, '%H:%M').time()
        if morning_start <= time_to_check <= morning_end and time_of_day != 'Morning':
            time_of_day = 'Morning'
            html += '<h3>Morning</h3>'
        elif afternoon_start <= time_to_check <= afternoon_end and time_of_day != 'Afternoon':
            time_of_day = 'Afternoon'
            html += '<h3>Afternoon</h3>'
        elif evening_start <= time_to_check <= evening_end and time_of_day != 'Evening':
            time_of_day = 'Evening'
            html += '<h3>Evening</h3>'
        #print(time_slot)

        parts = time_period.split(':')

        #html += f"""<h2>{header_time}</h2>"""
        wall_clock_time = ''
        for show in time_slot[time_period]['shows']:
            html += f"""<div class="wrapper">"""
            if wall_clock_time != show['wall_clock']:
                wall_clock_time = show['wall_clock']
                html += f"""<div class="float_left">{trim_time(show['wall_clock'])}</div>"""
            else:
                html += f"""<div class="empty_spacer">&nbsp;</div>"""
            num_entries = random.randint(0, 2)
            selected_entries = random.sample(non_channels, num_entries)
            selected_entries.append(show['channel'])
            for chan in sorted(selected_entries):
                if chan != show['channel']:
                    html += f"""<div class="border-box">{chan}</div>"""
                else:
                    html += f"""<div class="black-box">{chan}</div>"""
            specials_block = "<div class='special_box'>SPECIAL</div>" if show['type'] == 'specials' else ""
            staring = f"Cast: {show['stars']}" if show['stars'] is not None else ""
            html += f"""<div class="title_div">{show['title']}</div><div>&nbsp;-&nbsp;{show['genre']}</div>
            {specials_block}<div class="description_div">{show['description']} {staring} ({show['display_duration']})</div>
            </div>"""

    html += '</body></html>'
    write_to_file(html, 'current.html')


if __name__ == '__main__':
    play_lists = get_m3u_files(DIRECTORY)
    all_playlists = []
    for play_list in play_lists:
        channel = channel_number(play_list)
        all_playlists = all_playlists + parse_m3u8(f"{DIRECTORY}/{play_list}", START_TIME, channel)

    metadata = populate_meta(all_playlists)

    merged_meta = merge_meta(all_playlists, metadata)
    normalized = normalize_meta(merged_meta)
    normalized.sort(key=get_sort_key)
    grouped_by_hour = groupby(normalized, key=get_group_key)

    playlists_json = get_json(grouped_by_hour)
    playlist_html = get_html(playlists_json)
    print(playlist_html)

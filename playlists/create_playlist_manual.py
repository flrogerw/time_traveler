#!/usr/bin/env python3

import os
import random
import argparse
import logging
import datetime
import re
import psycopg
from psycopg import Connection as connection
from dotenv import load_dotenv
from playlists.classes.Channels import Channels
from playlists.classes.Playlists import Playlists
from playlists.classes.Shows import Shows
from playlists.classes.Episodes import Episodes
from playlists.classes.Schedules import Schedules

# Load environment variables from .env file
load_dotenv()

DEV_MODE = False

START_RE = re.compile(r'#EXTVLCOPT:start-time=([\d.]+)')
STOP_RE = re.compile(r'#EXTVLCOPT:stop-time=([\d.]+)')
IMAGE_DURATION_RE = re.compile(r'#EXTVLCOPT:image-duration=([\d.]+)')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_m3u_runtime(m3u_path: str) -> float:
    total_seconds = 0.0

    current_start = None
    current_stop = None
    current_image_duration = None

    with open(m3u_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            # Parse options
            if match := START_RE.match(line):
                current_start = float(match.group(1))

            elif match := STOP_RE.match(line):
                current_stop = float(match.group(1))

            elif match := IMAGE_DURATION_RE.match(line):
                current_image_duration = float(match.group(1))

            # Media path line → commit duration
            elif line and not line.startswith("#"):
                if current_image_duration is not None:
                    total_seconds += current_image_duration
                elif current_start is not None and current_stop is not None:
                    total_seconds += (current_stop - current_start)
                else:
                    raise ValueError(f"Missing timing info before media line: {line}")

                # Reset for next entry
                current_start = None
                current_stop = None
                current_image_duration = None

    return total_seconds


def format_runtime(seconds: float) -> str:
    seconds = int(round(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def get_db_connection() -> connection:
    """
    Establish and return a PostgreSQL database connection using environment variables.

    Returns:
        psycopg.Connection: PostgreSQL database connection object.
    """
    try:
        conn = psycopg.connect(
            dbname=os.getenv('DATABASE'),
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


def parse_arguments() -> argparse.Namespace:
    """
    Parse and return command-line arguments.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    today = datetime.datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument('--session_name', required=True, type=str,
                        help='The name of the session to process')
    parser.add_argument('--year', required=False, type=int, default=1960, choices=range(1960, 1990),
                        help='Broadcast year between 1960 and 1989.')


    args = parser.parse_args()
    logging.info(f"Parsed arguments: {args}")
    return args


def normalize_minutes(time_str: str, start_hour: float) -> int:
    """
    Convert HH:MM:SS string to minutes since schedule start,
    treating times before start_hour as next day.
    """
    # Parse the string
    t = datetime.datetime.strptime(time_str, "%H:%M:%S")

    # Total minutes since midnight
    minutes = t.hour * 60 + t.minute

    # Start hour in minutes
    start_minutes = int(start_hour * 60)

    # If the time is before the schedule start, treat as next day
    if minutes < start_minutes:
        minutes += 24 * 60

    return minutes


def main() -> None:
    """
    Main entry point of the script. Generates a playlist based on command-line arguments.
    """
    args = parse_arguments()

    host_names = ['TV-CBS-2','TV-NBC-4','TV-SYN-5','TV-ABC-7','TV-SYN-9','TV-SYN-11','TV-SYN-13']

    with get_db_connection() as db:
        for host in host_names:
            # Instantiate class handlers with DB connection and hostname/year
            channel = Channels(db, host)
            episodes = Episodes(db, host, args.year)
            schedule = Schedules(db, host)
            playlists = Playlists(db, host, DEV_MODE)

            # Network shows: use predefined schedules and fallback to generated time slots
            time_slots = schedule.get_manual_time_slots(channel.id, args.session_name)
            final_episodes = episodes.get_manual_episodes(time_slots)
            final_episodes.sort(
                key=lambda x: normalize_minutes(x["time_slot"], 19)
            )
            # Build final playlist
            playlists.get_playlist(final_episodes, holiday=None)
            if DEV_MODE:
                m3u_file = f"../playlists/{host}_playlist.m3u"
                total = parse_m3u_runtime(m3u_file)

                print(host)
                print(f"Total seconds: {total:.3f}")
                print(f"Total runtime: {format_runtime(total)}")


if __name__ == "__main__":
    main()

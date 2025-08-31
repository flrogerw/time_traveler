#!/usr/bin/env python3

import os
import random
import argparse
import logging
from datetime import datetime, timedelta
from pprint import pprint
from typing import Optional

import psycopg2
from psycopg2.extensions import connection
from dotenv import load_dotenv

from playlists.classes.Channels import Channels
from playlists.classes.Playlists import Playlists
from playlists.classes.Shows import Shows
from playlists.classes.Episodes import Episodes
from playlists.classes.Schedules import Schedules
from playlists.classes.Movies import Movies
from playlists.classes.Specials import Specials

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_db_connection() -> connection:
    """
    Establish and return a PostgreSQL database connection using environment variables.

    Returns:
        psycopg2.extensions.connection: PostgreSQL database connection object.
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


def parse_arguments() -> argparse.Namespace:
    """
    Parse and return command-line arguments.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    today = datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument('--hostname', required=True, type=str,
                        help='Host making the request (e.g., TV-ABC-7).')
    parser.add_argument('--year', required=True, type=int, default=1960, choices=range(1960, 1990),
                        help='Broadcast year between 1960 and 1989.')
    parser.add_argument('--dow', type=int, default=today.weekday(), choices=range(0, 7),
                        help='Day of the week (0=Mon, ..., 6=Sun). Defaults to today.')
    parser.add_argument('--start', default='19:00:00', type=str,
                        help='Start time for the playlist (default: 19:00:00).')
    parser.add_argument('--duration', default='05:00:00', type=str,
                        help='Duration of the playlist (default: 5 hours).')
    parser.add_argument('--holiday', required=False, default=None, type=str,
                        choices=['christmas', 'thanksgiving'],
                        help='Optional holiday to build a special playlist.')

    args = parser.parse_args()
    logging.info(f"Parsed arguments: {args}")
    return args


def main() -> None:
    """
    Main entry point of the script. Generates a playlist based on command-line arguments.
    """
    args = parse_arguments()

    with get_db_connection() as db:
        # Instantiate class handlers with DB connection and hostname/year
        channel = Channels(db, args.hostname)
        shows = Shows(db, args.hostname)
        episodes = Episodes(db, args.hostname, args.year)
        schedule = Schedules(db, args.hostname)
        playlists = Playlists(db, args.hostname)
        movies = Movies(db, args.year)
        specials = Specials(db)

        final_episodes = []

        # Handle special holiday playlist generation
        if args.holiday:
            current_sum = 0
            holiday_episodes = episodes.get_episodes_for_holiday(args.holiday)
            holiday_movies = movies.get_holiday_movies(args.holiday)
            holiday_specials = specials.get_holiday_specials(args.holiday)

            # Combine and shuffle all media types
            merged_media = holiday_episodes + holiday_movies + holiday_specials
            random.shuffle(merged_media)

            # Convert duration string to total seconds
            time_obj = datetime.strptime(args.duration, '%H:%M:%S')
            total_seconds = int(timedelta(hours=time_obj.hour, minutes=time_obj.minute, seconds=time_obj.second).total_seconds())

            # Accumulate items until duration is met or exceeded
            for item in merged_media:
                current_sum += item['duration']
                final_episodes.append(item)
                if current_sum >= total_seconds:
                    break

        elif args.hostname.split('-')[1] == 'SYN':
            # Syndicated shows: retrieve time slots and associated episodes
            time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, args.dow)
            if not time_slots:
                schedule.generate_time_slots(channel.id, args.year, args.dow)
                time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, args.dow)

            for slot in time_slots:
                episodes_for_slot = episodes.get_episodes_for_slot(
                    slot, channel.id, args.dow, args.year, args.hostname.split('-')[1]
                )
                final_episodes.extend(episodes_for_slot)

            final_episodes.sort(key=lambda x: x['time_slot'])

        else:
            # Network shows: use predefined schedules and fallback to generated time slots
            time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, args.dow)
            schedule_data = shows.get_scheduled_shows_id(args.year, args.dow)

            if not time_slots:
                # Attempt to generate from show schedule data if still no time slots
                current_time_slots = shows.calculate_time_differences(schedule_data)
                schedule.generate_time_slots(channel.id, args.year, args.dow, current_time_slots)
                time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, args.dow)

            logging.info(f"TIME SLOTS: {time_slots}")

            for slot in time_slots:
                episodes_for_slot = episodes.get_episodes_for_slot(
                    slot, channel.id, args.dow, args.year, args.hostname.split('-')[1], schedule_data
                )
                logging.debug(f"Episodes for slot {slot}: {episodes_for_slot}")
                final_episodes.extend(episodes_for_slot)

            final_episodes.sort(key=lambda x: x['time_slot'])

        # Build final playlist
        playlists.get_playlist(final_episodes, args.holiday)


if __name__ == "__main__":
    main()

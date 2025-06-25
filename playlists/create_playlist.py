#!/usr/bin/env python3
import os
import random
import psycopg2
import argparse
import logging
from playlists.classes.Channels import Channels
from playlists.classes.Playlists import Playlists
from playlists.classes.Shows import Shows
from playlists.classes.Episodes import Episodes
from playlists.classes.Schedules import Schedules
from playlists.classes.Movies import Movies
from datetime import datetime, timedelta
from dotenv import load_dotenv

from playlists.classes.Specials import Specials

load_dotenv()
# Configure logging
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
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for script execution.

    :return: Parsed arguments as an argparse Namespace object.
    """
    today = datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument('--hostname', dest='hostname', required=True, type=str,
                        help='Host making the request (e.g., TV-ABC-7).')
    parser.add_argument('--year', dest='year', default=1960, required=True, type=int, choices=range(1960, 1990),
                        help='Broadcast year between 1965 and 1989.')
    parser.add_argument('--dow', type=int, default=today.weekday(), choices=range(0, 7),
                        help='Day of the week (0=Mon, ..., 6=Sun). Defaults to the current day.')
    parser.add_argument('--start', default='19:00:00', type=str, help='Start time (default: 19:00:00 or 7 PM).')
    parser.add_argument('--duration', default='05:00:00', type=str,
                        help='Duration of the playlist (default: 5 hours).')
    parser.add_argument('--holiday', required=False, dest='holiday', type=str, default=None,
                        choices=['christmas', 'thanksgiving'],
                        help='A holiday from which to create a playlist.')

    args = parser.parse_args()

    logging.info(f"Parsed arguments: {args}")
    return args


def main() -> None:
    """
    Main function to run the program. Retrieves time slots and episodes based on the specified parameters and
    generates a playlist.
    """
    args = parse_arguments()

    with get_db_connection() as db:
        # Instantiate required classes
        channel = Channels(db, args.hostname)
        shows = Shows(db, args.hostname)
        episodes = Episodes(db, args.hostname, args.year)
        schedule = Schedules(db, args.hostname)
        playlists = Playlists(db, args.hostname)
        movies = Movies(db, args.year)
        specials = Specials(db)

        final_episodes = []

        # Check if hostname is for syndicated shows or network shows
        if args.holiday:
            current_sum = 0
            holiday_episodes = episodes.get_episodes_for_holiday(args.holiday)
            holiday_movies = movies.get_holiday_movies(args.holiday)
            holiday_specials = specials.get_holiday_specials(args.holiday)

            random.shuffle(holiday_episodes)
            random.shuffle(holiday_movies)
            random.shuffle(holiday_specials)

            merged_media = holiday_episodes + holiday_movies + holiday_specials
            random.shuffle(merged_media)

            # Convert the slot duration to total seconds
            time_obj = datetime.strptime(args.duration, '%H:%M:%S')
            total_seconds = int(timedelta(hours=time_obj.hour, minutes=time_obj.minute,
                                          seconds=time_obj.second).total_seconds())
            for item in merged_media:
                current_sum += item['duration']
                final_episodes.append(item)

                if current_sum >= total_seconds:
                    break

        elif args.hostname.split('-')[1] == 'SYN':
            # Get time slots for syndicated shows
            time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, args.dow)
            if not time_slots:
                schedule.generate_time_slots(channel.id, args.year, args.dow)
                time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, args.dow)

            for slot in time_slots:
                episodes_for_slot = episodes.get_episodes_for_slot(slot, channel.id, args.dow, args.year,
                                                                   args.hostname.split('-')[1])
                final_episodes.extend(episodes_for_slot)

                # Sort final episodes by time slot
                final_episodes = sorted(final_episodes, key=lambda x: x['time_slot'])

        else:
            # Get time slots for network shows
            time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, args.dow)
            schedule_data = {}
            if not time_slots:
                # Generate time slots if not found
                schedule_data = shows.get_scheduled_shows_id(args.year, args.dow)
                current_time_slots = shows.calculate_time_differences(schedule_data)
                schedule.generate_time_slots(channel.id, args.year, args.dow, current_time_slots)
                time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, args.dow)
            print('TIME SLOTS ', time_slots)
            for slot in time_slots:
                episodes_for_slot = episodes.get_episodes_for_slot(slot, channel.id, args.dow, args.year,
                                                                   args.hostname.split('-')[1], schedule_data)
                print(episodes_for_slot)
                final_episodes.extend(episodes_for_slot)

                # Sort final episodes by time slot
                final_episodes = sorted(final_episodes, key=lambda x: x['time_slot'])

        # Generate playlist and print current schedule
        # print(final_episodes)
        playlists.get_playlist(final_episodes, args.holiday)
        # pprint(schedule.get_current_schedule(channel.id, args.dow))


if __name__ == "__main__":
    main()

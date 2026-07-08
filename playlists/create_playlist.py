#!/usr/bin/env python3

import os
import argparse
import logging
import datetime
from pprint import pprint
from typing import Optional

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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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

    parser.add_argument('--hostname', required=True, type=str,
                        help='Host making the request (e.g., TV-ABC-7).')
    parser.add_argument('--year', required=True, type=int, default=1960, choices=range(1960, 1990),
                        help='Broadcast year between 1960 and 1989.')
    parser.add_argument('--dow', type=int, default=None, choices=range(0, 7),
                        help='Day of the week (0=Mon, ..., 6=Sun). Defaults to the weekday of the '
                             'equivalent historical date for --sim-date.')
    parser.add_argument('--sim-date', type=str, default=today.strftime('%Y-%m-%d'),
                        help='Real calendar date (YYYY-MM-DD) to map onto --year for historical '
                             'schedule lookups. Defaults to today.')
    parser.add_argument('--start', default='19:00:00', type=str,
                        help='Start time for the playlist (default: 19:00:00).')
    parser.add_argument('--duration', default='05:00:00', type=str,
                        help='Duration of the playlist (default: 5 hours).')
    parser.add_argument('--holiday', required=False, default=None, type=str,
                        choices=['christmas', 'thanksgiving', 'easter', 'halloween'],
                        help='Optional holiday to build a special playlist.')

    args = parser.parse_args()
    logging.info(f"Parsed arguments: {args}")
    return args


def main() -> None:
    """
    Main entry point of the script. Generates a playlist based on command-line arguments.
    """
    args = parse_arguments()
    sim_date = datetime.datetime.strptime(args.sim_date, '%Y-%m-%d').date()
    eq_date, _ = Schedules.equivalent_date(args.year, sim_date.month, sim_date.day)
    dow = args.dow if args.dow is not None else eq_date.weekday()

    with get_db_connection() as db:
        # Instantiate class handlers with DB connection and hostname/year
        channel = Channels(db, args.hostname)
        shows = Shows(db, args.hostname)
        episodes = Episodes(db, args.hostname, args.year)
        schedule = Schedules(db, args.hostname)
        playlists = Playlists(db, args.hostname)

        final_episodes = []
        network_name = args.hostname.split('-')[1]
        schedule_data = {}

        # Determine time slots and (for network channels) the predefined historical show lineup.
        # channel.type ('network'/'syndicated') is the source of truth for this branch, not the
        # hostname string -- a channel's on-air branding doesn't always match how it's operated.
        if channel.type == 'syndicated':
            time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, dow)
            if not time_slots:
                schedule.generate_time_slots(channel.id, args.year, dow)
                time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, dow)
        else:
            time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, dow)
            schedule_data = shows.get_scheduled_shows_id(args.year, dow)

            if not time_slots:
                # Attempt to generate from show schedule data if still no time slots
                current_time_slots = shows.calculate_time_differences(schedule_data)
                schedule.generate_time_slots(channel.id, args.year, dow, current_time_slots)
                time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, dow)

            logging.info(f"TIME SLOTS: {time_slots}")

        # Holiday programming: pin whichever of the requested slots have a fitting holiday
        # episode, via the same schedule_overrides mechanism used for multi-part continuations,
        # rather than replacing the whole day -- slots without a fitting holiday episode fall
        # through to the normal engine below. (Movies/specials aren't supported here yet, since
        # schedule_overrides only references the episodes table.)
        if args.holiday:
            holiday_episodes = episodes.get_episodes_for_holiday(args.holiday) or []
            used_holiday_ids = set()
            for slot in time_slots:
                time_obj = datetime.datetime.strptime(slot['duration'], '%H:%M:%S')
                slot_seconds = int(datetime.timedelta(hours=time_obj.hour, minutes=time_obj.minute,
                                                       seconds=time_obj.second).total_seconds())
                candidates = [e for e in holiday_episodes if e['episode_id'] not in used_holiday_ids]
                if not candidates:
                    continue
                best = min(candidates, key=lambda e: abs(e['duration'] - slot_seconds))
                if abs(best['duration'] - slot_seconds) <= 600:  # within 10 minutes of the slot
                    used_holiday_ids.add(best['episode_id'])
                    schedule.override_slot(channel.id, eq_date, slot['start_time'], best['episode_id'])

        # Fill every slot through the normal engine -- holiday-pinned slots resolve via the
        # schedule_overrides priority check inside get_next_episode.
        prev_genre = None
        for slot in time_slots:
            episodes_for_slot = episodes.get_episodes_for_slot(
                slot, channel.id, dow, args.year, network_name, schedule_data, air_date=eq_date, prev_genre=prev_genre
            )
            logging.debug(f"Episodes for slot {slot}: {episodes_for_slot}")
            final_episodes.extend(episodes_for_slot)
            last_show_id = episodes_for_slot[-1].get('show_id') if episodes_for_slot else None
            if last_show_id is not None:
                prev_genre = shows.get_first_genre(last_show_id)

        final_episodes.sort(key=lambda x: x['time_slot'])

        # Build final playlist
        pprint(final_episodes)
        print(playlists.get_playlist(final_episodes, None))


if __name__ == "__main__":
    main()

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
from playlists.classes.SyndicatedScheduler import SyndicatedScheduler
from playlists.classes.Episodes import channel_airs_interstitials

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _plus_one_second(duration: str) -> str:
    """HH:MM:SS + 1s. Schedules.get_time_slots trims a second off the window and compares strictly,
    so a slot ending exactly at the window's end is dropped; the duration-aware grid fills the
    window exactly, so its last slot needs this nudge to be read back."""
    hours, minutes, seconds = map(int, duration.split(':'))
    total = hours * 3600 + minutes * 60 + seconds + 1
    return f"{total // 3600:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"


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
    parser.add_argument('--duration-aware', action='store_true',
                        help='EXPERIMENTAL: for syndicated daytime windows, build the slot grid around the '
                             'shows\' real lengths. Replaces overlapping time_slot_schedules rows for the '
                             'window, so it is off by default.')
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

    # A window can't cross midnight: the slot grid is per calendar day, and Schedules.get_time_slots matches
    # on time-of-day, so it would return nothing -- after which generate_time_slots would append another full
    # random grid on every run. Run the two halves separately (e.g. 23:00 for 01:00, then the next date at 00:00).
    h, m, sec = map(int, args.duration.split(':'))
    start_h, start_m, start_s = map(int, args.start.split(':'))
    if (start_h * 3600 + start_m * 60 + start_s) + (h * 3600 + m * 60 + sec) > 86400:
        raise SystemExit(f"--start {args.start} --duration {args.duration} crosses midnight; run it as two windows "
                         f"(through 24:00 on {args.sim_date}, then from 00:00 on the next date).")

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
            syndicated_scheduler = SyndicatedScheduler(db)

            # Daytime windows (weekday 06:00-19:00, weekend 07:00-19:00) are built AROUND the shows'
            # real lengths: a show is picked for the current time, its slot is sized from its own
            # show_duration, and the next show starts when it ends -- instead of fixing a random
            # slot grid first and fitting shows into it. Returns None (keep the flow below) when the
            # window is outside that scope or already templated, so re-runs reuse the stored grid.
            built = (
                syndicated_scheduler.build_daytime_schedule(channel.id, args.year, dow, args.start, args.duration)
                if args.duration_aware else None
            )
            if built is not None:
                _, schedule_data = built
                time_slots = schedule.get_time_slots(
                    channel.id, args.year, args.start, _plus_one_second(args.duration), dow)
            else:
                time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, dow)
                if not time_slots:
                    schedule.generate_time_slots(channel.id, args.year, dow)
                    time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, dow)

            # Era-aware syndicated daytime scheduling (see CLAUDE_SYNDICATED_SCHEDULING_HANDOFF.md
            # on the Desktop): resolve each slot to a show_id via fixed slots / weighted era-genre
            # fill rules, and feed it through the exact same schedule_data mechanism the network
            # branch already uses below -- Episodes.get_episodes_for_slot() falls back to
            # Shows.get_available_show_id() unchanged whenever resolve_syndicated_slot() can't
            # resolve a slot (e.g. outside the 06:00-19:00 weekday daypart system, or a fully
            # starved category with no eligible show), so this never needs its own fallback path.
            already_used_show_ids = set()
            prev_show_id = None
            for slot in (time_slots if built is None else []):
                slot_time = datetime.datetime.strptime(slot['start_time'], '%H:%M:%S').time()
                show_id = syndicated_scheduler.resolve_syndicated_slot(
                    channel.id, args.year, dow, slot_time, args.year, already_used_show_ids,
                    prev_show_id=prev_show_id
                )
                prev_show_id = show_id
                if show_id is not None:
                    schedule_data[slot['start_time']] = show_id
                    already_used_show_ids.add(show_id)
        else:
            time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, dow)
            schedule_data = shows.get_scheduled_shows_id(eq_date, dow)

            if not time_slots:
                # Attempt to generate from show schedule data if still no time slots
                current_time_slots = shows.calculate_time_differences(schedule_data)
                schedule.generate_time_slots(channel.id, args.year, dow, current_time_slots)
                time_slots = schedule.get_time_slots(channel.id, args.year, args.start, args.duration, dow)

            # Re-point an already-templated slot at schedule_data's show whenever a mid-season
            # historical switch (see migrate_schedule_date_ranges.py) means today's eq_date
            # resolves to a different show than whatever first templated this slot -- otherwise
            # schedule_template's "first run wins for the whole replication_year" caching would
            # silently keep airing the earlier show forever. Network-only; see
            # sync_schedule_template_show_id's docstring for why this must not apply to syndicated.
            for start_time, show_id in schedule_data.items():
                if show_id is not None:
                    schedule.sync_schedule_template_show_id(channel.id, dow, args.year, start_time, show_id)

            logging.info(f"TIME SLOTS: {time_slots}")

        # Holiday programming: pin whichever of the requested slots have a fitting holiday
        # episode, via the same schedule_overrides mechanism used for multi-part continuations,
        # rather than replacing the whole day -- slots without a fitting holiday episode fall
        # through to the normal engine below. (Movies/specials aren't supported here yet, since
        # schedule_overrides only references the episodes table.)
        if args.holiday:
            holiday_episodes = episodes.get_episodes_for_holiday(args.holiday) or []
            # Never reuse an episode another channel already holds (hard rule); avoid reusing a
            # show across channels/slots whenever another fitting show exists (soft rule).
            used_holiday_ids, used_holiday_shows = episodes.get_holiday_episodes_in_use(args.holiday, args.year)
            for slot in time_slots:
                time_obj = datetime.datetime.strptime(slot['duration'], '%H:%M:%S')
                slot_seconds = int(datetime.timedelta(hours=time_obj.hour, minutes=time_obj.minute,
                                                       seconds=time_obj.second).total_seconds())
                fitting = [e for e in holiday_episodes
                           if e['episode_id'] not in used_holiday_ids
                           and abs(e['duration'] - slot_seconds) <= 600]  # within 10 minutes of the slot
                if not fitting:
                    continue
                fresh = [e for e in fitting if e['show_id'] not in used_holiday_shows]
                best = min(fresh or fitting, key=lambda e: abs(e['duration'] - slot_seconds))
                used_holiday_ids.add(best['episode_id'])
                used_holiday_shows.add(best['show_id'])
                schedule.override_slot(channel.id, eq_date, slot['start_time'], best['episode_id'])

        # Fill every slot through the normal engine -- holiday-pinned slots resolve via the
        # schedule_overrides priority check inside get_next_episode.
        prev_genre = None
        for slot_index, slot in enumerate(time_slots):
            episodes_for_slot = episodes.get_episodes_for_slot(
                slot, channel.id, dow, args.year, network_name, schedule_data, air_date=eq_date, prev_genre=prev_genre
            )
            logging.debug(f"Episodes for slot {slot}: {episodes_for_slot}")
            # get_next_episode loads time_slot/replication_year by joining EVERY broadcast_log row for
            # the episode, so an episode aired before (another channel or year) can carry that old
            # row's values. Stamp the slot's real ones: they drive the playlist's sort order, the
            # era of the commercials, and which audience rules apply.
            slot_time_of_day = datetime.datetime.strptime(slot['start_time'], '%H:%M:%S').time()
            for scheduled in episodes_for_slot:
                scheduled['time_slot'] = slot_time_of_day
                scheduled['replication_year'] = slot['schedule_id']
            final_episodes.extend(episodes_for_slot)

            # Interstitials (e.g. School House Rock, Saturday mornings) air in the break between
            # two programs, never in a slot of their own: one after this slot's programming if
            # another program follows and an interstitial is eligible for that next slot's time.
            # Syndicated channels and the ABC network channel carry them (see INTERSTITIAL_NETWORKS).
            if (channel_airs_interstitials(channel.type, network_name)
                    and slot_index + 1 < len(time_slots) and episodes_for_slot):
                next_start = datetime.datetime.strptime(time_slots[slot_index + 1]['start_time'], '%H:%M:%S').time()
                interstitial_show_id = episodes.get_interstitial_show_id(args.year, dow, next_start)
                if interstitial_show_id is not None:
                    interstitial = episodes.get_next_interstitial(
                        interstitial_show_id, channel.id, args.year, slot['start_time']
                    )
                    if interstitial:
                        final_episodes.append(interstitial)
            last_show_id = episodes_for_slot[-1].get('show_id') if episodes_for_slot else None
            if last_show_id is not None:
                prev_genre = shows.get_first_genre(last_show_id)

        final_episodes.sort(key=lambda x: x['time_slot'])

        # Build final playlist
        pprint(final_episodes)
        print(playlists.get_playlist(final_episodes, None, dow=dow))


if __name__ == "__main__":
    main()

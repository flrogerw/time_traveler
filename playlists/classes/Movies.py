#!/usr/bin/env python3
import logging
from psycopg.rows import dict_row
from datetime import datetime, timedelta
from psycopg import sql

# Configure logging to capture important events and errors
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Movies:
    def __init__(self, db, year):
        """
        Initialize the Movies class with a database connection and the specified year.

        :param db: The database connection object.
        :param year: The year for which to filter movies.
        """
        self.db_connection = db
        self.cur = db.cursor(row_factory=dict_row)  # Use dict_row to fetch results as dictionaries.
        self.year = year

    def get_holiday_movies(self, holiday):
        try:
            query = sql.SQL("""SELECT *, NULL AS time_slot, end_point - start_point as duration
                                       FROM public.movies
                                       WHERE movie_season = %s
                                       ORDER BY RANDOM();""")

            # Execute the query and retrieve the next episode
            self.cur.execute(query, (holiday,))
            # Parse the 'duration' from the time slot into a datetime object for time calculations.
            time_obj = datetime.strptime('02:00:00', '%H:%M:%S')
            # Convert the time duration into total seconds for easier manipulation.
            total_seconds = timedelta(hours=time_obj.hour, minutes=time_obj.minute,
                                      seconds=time_obj.second).total_seconds()

            # Process the fetched movie records into a list of dictionaries and add extra slot information.
            formatted_records = [{
                **dict(record),  # Unpack the record into a dictionary.
                'type': 'movie',  # Label the entry as a movie.
                'time_slot': datetime.strptime('01:00:00', '%H:%M:%S').time(),
                # Extract and format the start time.
                'slot_duration': int(total_seconds)  # Add the duration of the time slot in seconds.
            } for record in self.cur.fetchall()]

        except Exception:
            # Log any errors encountered during the query
            logging.exception(f"Could not get_holiday_movies for {holiday}.")
        else:
            return formatted_records

    def get_random_movie(self, duration, slot):
        """
        Retrieve a random, not-yet-shown-on-this-channel movie for the given duration and time
        slot, and record the pick in broadcast_log so it isn't repeated and so the printed guide
        can show which movie actually aired (mirrors Episodes.get_next_episode's bookkeeping).

        :param duration: The maximum allowed duration for the movie.
        :param slot: A dictionary containing the time slot details, including 'channel_id',
                     'duration' and 'start_time'.
        :return: A dictionary representing the selected movie with additional information about
                 the time slot, or None if no unshown movie fits.
        """
        # SQL query to select a movie with a duration shorter than the specified value, released
        # before the given year, and not already shown on this channel.
        query = """SELECT * FROM movies
                   WHERE end_point < %s AND movie_release_date <= %s
                   AND movie_season = 'any'
                   AND movie_id NOT IN (
                       SELECT episode_id FROM broadcast_log WHERE channel_id = %s AND show_id = 173
                   )
                   ORDER BY RANDOM() LIMIT 1"""

        # Execute the query with the provided duration, year, and channel.
        self.cur.execute(query, (duration, self.year, slot['channel_id']))
        record = self.cur.fetchone()
        if record is None:
            return None

        # Parse the 'duration' from the time slot into a datetime object for time calculations.
        time_obj = datetime.strptime(slot['duration'], '%H:%M:%S')
        # Convert the time duration into total seconds for easier manipulation.
        total_seconds = timedelta(hours=time_obj.hour, minutes=time_obj.minute, seconds=time_obj.second).total_seconds()

        formatted_record = {
            **dict(record),  # Unpack the record into a dictionary.
            'type': 'movie',  # Label the entry as a movie.
            'time_slot': datetime.strptime(slot['start_time'], '%H:%M:%S').time(),  # Extract and format the start time.
            'slot_duration': int(total_seconds),  # Add the duration of the time slot in seconds.
            'replication_year': slot['schedule_id']  # Needed by Playlists.get_playlist's commercial lookup.
        }

        self.cur.execute(
            """INSERT INTO broadcast_log (channel_id, show_id, episode_id, date_played, time_slot, replication_year)
               VALUES (%s, 173, %s, current_date, %s, %s);""",
            (slot['channel_id'], record['movie_id'], slot['start_time'], slot['schedule_id']))
        self.db_connection.commit()

        return formatted_record

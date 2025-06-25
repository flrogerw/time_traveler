#!/usr/bin/env python3
import logging
from psycopg2.extras import DictCursor
from datetime import datetime, timedelta
from psycopg2 import sql

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
        self.cur = db.cursor(cursor_factory=DictCursor)  # Use DictCursor to fetch results as dictionaries.
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
        Retrieve a random movie based on the specified duration and time slot.

        :param duration: The maximum allowed duration for the movie.
        :param slot: A dictionary containing the time slot details, including 'duration' and 'start_time'.
        :return: A dictionary representing the selected movie with additional information about the time slot.
        """
        # SQL query to select a movie with a duration shorter than the specified value and released before the given year.
        query = """SELECT * FROM movies 
                   WHERE end_point < %s AND movie_release_date <= %s
                   AND movie_season = 'any' 
                   ORDER BY RANDOM() LIMIT 1"""

        # Execute the query with the provided duration and year.
        self.cur.execute(query, (duration, self.year))

        # Parse the 'duration' from the time slot into a datetime object for time calculations.
        time_obj = datetime.strptime(slot['duration'], '%H:%M:%S')
        # Convert the time duration into total seconds for easier manipulation.
        total_seconds = timedelta(hours=time_obj.hour, minutes=time_obj.minute, seconds=time_obj.second).total_seconds()

        # Process the fetched movie records into a list of dictionaries and add extra slot information.
        formatted_record = [{
            **dict(record),  # Unpack the record into a dictionary.
            'type': 'movie',  # Label the entry as a movie.
            'time_slot': datetime.strptime(slot['start_time'], '%H:%M:%S').time(),  # Extract and format the start time.
            'slot_duration': int(total_seconds)  # Add the duration of the time slot in seconds.
        } for record in self.cur.fetchall()]

        # Return the first formatted movie record (there should only be one due to the LIMIT 1 in the query).
        return formatted_record[0]

# Class to handle specials-related database operations
import logging
from datetime import datetime, timedelta

from psycopg2.extras import DictCursor
from psycopg2 import sql


class Specials:
    def __init__(self, db):
        self.db_connection = db
        self.cur = db.cursor(cursor_factory=DictCursor)

    def get_holiday_specials(self, holiday):
        try:
            query = sql.SQL("""SELECT *, end_point - start_point as duration
                                    FROM specials
                                    WHERE specials_season = %s""")

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
                'type': 'special',  # Label the entry as a movie.
                'time_slot': datetime.strptime('01:00:00', '%H:%M:%S').time(),
                # Extract and format the start time.
                'slot_duration': int(total_seconds)  # Add the duration of the time slot in seconds.
            } for record in self.cur.fetchall()]

        except Exception as e:
            # Log any errors encountered during the query
            logging.exception(f"Could not get Next Episode")
        else:
            return formatted_records

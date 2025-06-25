#!/usr/bin/env python3
from psycopg2.extras import execute_values, DictCursor
import random
from datetime import datetime, time, timedelta
from playlists.classes.Shows import Shows


class Schedules:
    def __init__(self, db, hostname):
        """
        Initialize the Schedules class with a database connection and hostname.

        :param db: The database connection object.
        :param hostname: Hostname for use in Shows class initialization.
        """
        # Define possible durations in seconds (e.g., 30 mins, 1 hour)
        self.durations = [1800, 3600]
        # Weights for the random choice of durations (favoring shorter durations)
        self.weights = [0.8, 0.3]
        self.db_connection = db
        self.shows = Shows(self.db_connection, hostname)
        self.cur = db.cursor(cursor_factory=DictCursor)  # Use DictCursor to fetch results as dictionaries.

    def get_current_schedule(self, channel_id, dow):
        """
        Retrieve the current schedule for a specific channel and day of the week.

        :param channel_id: The ID of the channel.
        :param dow: The day of the week (integer).
        :return: A list of schedule records for the channel on the specified day.
        """
        # Query to get the distinct schedule information for the specified channel and day of the week.
        self.cur.execute("""
            SELECT DISTINCT ON (st.channel_id, st.show_id, st.time_slot)
                split_part(ch.channel_name, '-', 3) AS channel,
                st.time_slot,
                sh.show_duration,
                sh.show_name AS show,
                e.episode_description AS description,
                bl.date_played
            FROM schedule_template st
            LEFT JOIN broadcast_log bl ON st.channel_id = bl.channel_id AND st.show_id = bl.show_id
            LEFT JOIN episodes e ON bl.episode_id = e.episode_id
            LEFT JOIN shows sh ON st.show_id = sh.show_id
            LEFT JOIN channels ch ON ch.channel_id = bl.channel_id
            WHERE st.channel_id = %s
            AND %s = ANY(st.days_of_week)
            ORDER BY st.channel_id, st.time_slot, st.show_id, bl.date_played DESC;""",
                         (channel_id, dow))
        return self.cur.fetchall()

    def get_time_slots(self, channel_id, replication_year, start_time_str, duration_str, dow):
        """
        Retrieve the time slots for a given channel and replication year based on start time and duration.

        :param channel_id: The ID of the channel.
        :param replication_year: The replication year to consider.
        :param start_time_str: The start time in string format (HH:MM:SS).
        :param duration_str: The duration in string format (HH:MM:SS).
        :return: A list of formatted time slot records.
        """
        try:
            # Parse start time and duration strings into appropriate datetime/timedelta objects.
            start_time = datetime.strptime(start_time_str, "%H:%M:%S").time()
            hours, minutes, seconds = map(int, duration_str.split(':'))
            duration = timedelta(hours=hours, minutes=minutes, seconds=seconds - 1)
            start_datetime = datetime.combine(datetime.today(), start_time)
            end_datetime = start_datetime + duration

            # Query to retrieve time slots that match the given channel, year, and time range.
            self.cur.execute("""
                SELECT * FROM time_slot_schedules
                WHERE channel_id = %s
                AND schedule_id = %s
                AND (start_time <= CAST(%s AS TIME) 
                AND (start_time + (duration - CAST('00:00:01' AS TIME))) > CAST(%s AS TIME))
                AND channel_dow = %s
                ORDER BY start_time ASC;""",
                             (channel_id, replication_year, end_datetime.time(), start_time_str, dow))

            # Format the results into a list of dictionaries with human-readable times.
            records = self.cur.fetchall()
            formatted_records = [{
                **dict(record),  # Unpack the record into a dictionary.
                'start_time': record['start_time'].strftime('%H:%M:%S'),
                'end_time': record['end_time'].strftime('%H:%M:%S'),
                'duration': str(self.timedelta_to_hms(record['duration']))  # Format duration.
            } for record in records]
            return formatted_records
        except Exception as e:
            print(f"Error fetching time slots: {e}")
            return []

    @staticmethod
    def timedelta_to_hms(td):
        """
        Convert a timedelta object into an H:M:S string format.

        :param td: The timedelta object.
        :return: A string representation of the duration in H:M:S format.
        """
        try:
            total_seconds = int(td.total_seconds())
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return f'{hours:02}:{minutes:02}:{seconds:02}'
        except Exception as e:
            print(f"Error converting timedelta to H:M:S: {e}")
            return "00:00:00"

    def generate_time_slots(self, channel_id, replication_year, dow, current_schedule=None):
        """
        Generate and insert time slots into the time_slot_schedules table based on the given channel and schedule.

        :param channel_id: The ID of the channel.
        :param replication_year: The replication year to consider.
        :param dow: The day of the week (integer).
        :param current_schedule: Optional current schedule to consider.
        """
        try:
            values = []
            # Generate the schedule (randomized or predefined).
            schedule = self.generate_schedule(current_schedule)
            for start_str, duration_seconds in schedule:
                start_time = time(int(start_str.split(":")[0]), int(start_str.split(":")[1]))
                duration = timedelta(seconds=duration_seconds)
                values.append((replication_year, channel_id, start_time, duration, dow))

            # Insert generated time slots into the database.
            execute_values(self.cur, """
                INSERT INTO time_slot_schedules (schedule_id, channel_id, start_time, duration, channel_dow)
                VALUES %s""", values)
        except Exception as e:
            print(f"Error generating time slots: {e}")
        finally:
            self.db_connection.commit()

    def generate_schedule(self, predefined_slots=None):
        """
        Generate a full day's schedule, either filling gaps between predefined slots or entirely random.

        :param predefined_slots: Optional dictionary of predefined slots.
        :return: A list of tuples representing the schedule (start time and duration).
        """
        try:
            schedule = []
            total_seconds = 0
            current_time = timedelta(hours=0)

            if predefined_slots:
                for time_str, duration in sorted(predefined_slots.items()):
                    hour, minute, second = map(int, time_str.split(':'))
                    slot_time = timedelta(hours=hour, minutes=minute, seconds=second)

                    # Fill gaps between predefined slots.
                    while current_time < slot_time and total_seconds < 86400:
                        gap_duration = random.choices(self.durations, self.weights)[0]
                        if total_seconds + gap_duration > slot_time.total_seconds():
                            gap_duration = (slot_time - current_time).total_seconds()

                        schedule.append((str(current_time), gap_duration))
                        total_seconds += gap_duration
                        current_time += timedelta(seconds=gap_duration)

                    if duration is not None and total_seconds < 86400:
                        schedule.append((time_str, duration))
                        total_seconds += duration
                        current_time = slot_time + timedelta(seconds=duration)

            # Generate random slots to fill the schedule until 24 hours (86400 seconds).
            while total_seconds < 86400:
                duration = random.choices(self.durations, self.weights)[0]
                if total_seconds + duration > 86400:
                    duration = 86400 - total_seconds

                time_str = str(current_time)
                schedule.append((time_str, duration))

                total_seconds += duration
                current_time += timedelta(seconds=duration)

            return schedule
        except Exception as e:
            print(f"Error generating schedule: {e}")
            return []

    def insert_time_slot_schedule(self, replication_year, channel_id, start_str, duration_delta, dow):
        """
        Insert a single time slot schedule into the database.

        :param replication_year: The replication year.
        :param channel_id: The ID of the channel.
        :param start_str: The start time in string format (HH:MM).
        :param duration_delta: The duration as a timedelta object.
        :param dow: The day of the week (integer).
        """
        try:
            start_time = time(int(start_str.split(":")[0]), int(start_str.split(":")[1]))
            execute_values(self.cur, """
                INSERT INTO time_slot_schedules (schedule_id, channel_id, start_time, duration, channel_dow)
                VALUES %s""", [(replication_year, channel_id, start_time, duration_delta, dow)])
        except Exception as e:
            print(f"Error inserting time slot schedule: {e}")
        finally:
            self.db_connection.commit()

    def insert_schedule_template(self, time_slot, dow, network, show_id=None):
        """
        Inserts or updates a schedule template entry for a specific time slot.

        Args:
            time_slot (dict): A dictionary containing the time slot details (e.g., 'channel_id', 'start_time', 'duration', 'schedule_id').
            dow (int): The day of the week for the schedule.
            network (str): The network identifier (e.g., 'SYN' for syndication).
            show_id (int, optional): The ID of the show to schedule. If not provided, an available show ID will be fetched.

        Returns:
            None
        """
        try:
            # Convert the 'duration' from the time_slot dictionary into a datetime object
            time_obj = datetime.strptime(time_slot['duration'], '%H:%M:%S')

            # Calculate the total duration of the show in seconds
            total_seconds = time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second

            # If a show_id is not provided, get an available show ID based on channel and schedule information
            show_id = show_id if show_id else self.shows.get_available_show_id(
                time_slot['channel_id'],  # Channel ID
                time_slot['schedule_id'],  # Schedule ID
                total_seconds,  # Duration in seconds
                network  # Network identifier
            )

            if show_id:
                # Define weekdays (0 = Monday, ..., 4 = Friday)
                weekdays = [0, 1, 2, 3, 4]

                # Determine the days of the week the show should be scheduled.
                # If the day of week (dow) is a weekday and the network is 'SYN', apply to all weekdays
                # Otherwise, apply only to the specified day of the week (dow)
                days_of_week = weekdays if dow in weekdays and network == 'SYN' else [dow]

                # Insert the schedule template into the database.
                # If the same channel, time slot, and replication year already exist, update the existing entry.
                self.cur.execute("""
                    INSERT INTO schedule_template (channel_id, time_slot, show_id, runtime, replication_year, days_of_week)
                    VALUES (%s, %s, %s, %s, %s, %s) 
                    ON CONFLICT (channel_id, time_slot, replication_year, days_of_week)
                    DO UPDATE SET time_slot = EXCLUDED.time_slot, show_id = EXCLUDED.show_id, runtime = EXCLUDED.runtime;
                """, (
                    time_slot['channel_id'],  # Channel ID from the time_slot
                    time_slot['start_time'],  # Start time of the show
                    show_id,  # ID of the show to be scheduled
                    time_slot['duration'],  # Duration of the show in 'HH:MM:SS' format
                    time_slot['schedule_id'],  # Replication year or schedule identifier
                    days_of_week  # List of days of the week for the schedule
                ))
        except Exception as e:
            # Print an error message if something goes wrong during the insertion
            print(f"Error generating and inserting schedule: {e}")
        finally:
            # Commit the transaction to the database to save changes
            self.db_connection.commit()

    def delete_row(self, row_id):
        """
        Delete a specific row from the time_slot_schedules table.

        :param row_id: The ID of the row to delete.
        """
        try:
            self.cur.execute("""DELETE FROM time_slot_schedules WHERE id = %s""", (row_id,))
            self.db_connection.commit()
        except Exception as e:
            print(f"Error deleting record: {row_id}. Exception: {e}")

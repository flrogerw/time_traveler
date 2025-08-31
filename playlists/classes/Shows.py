#!/usr/bin/env python3
from psycopg2.extras import DictCursor
import calendar
from datetime import datetime, timedelta

# Constants
SCHEDULE_RECURSION = 3  # Number of past years to consider for schedule recursion
MOVIE_ID = 173  # Special ID for movies, used in time calculations


class Shows:
    def __init__(self, db, hostname):
        # Initialize database connection and parse hostname for network/channel details
        self.db_connection = db
        _, self.network, self.channel = hostname.split('-')
        # Create a cursor using DictCursor to fetch rows as dictionaries
        self.cur = db.cursor(cursor_factory=DictCursor)

    # Fetch the duration of a specific show based on its ID and a minimum required duration
    def get_show_duration(self, show_id, duration=1500):
        query = """SELECT duration
                   FROM (SELECT unnest(show_duration) AS duration FROM shows WHERE show_id = %s) AS expanded
                   WHERE duration >= %s
                   ORDER BY duration ASC
                   LIMIT 1;"""
        self.cur.execute(query, (show_id, duration))
        return self.cur.fetchone()[0]

    # Fetch show counts grouped by duration for a specific channel and year
    def get_show_count_by_duration(self, channel_id, year):
        self.cur.execute("""
        SELECT show_duration, COUNT(*) AS count 
        FROM shows s
        WHERE s.show_type != 'children'
        AND EXTRACT(YEAR FROM s.airdate_end) <= %s
        AND s.show_id NOT IN (
                SELECT show_id FROM schedule_template WHERE channel_id = %s AND replication_year = %s
            )
        GROUP BY s.show_duration""", (year - SCHEDULE_RECURSION, channel_id, year))
        return self.cur.fetchall()

    # Get an available show ID based on channel, year, required runtime, and network
    def get_available_show_id(self, channel_id, year, required_runtime, network):
        # Condition for whether the show is syndicated or belongs to a specific network
        where_string = 's.show_is_syndicated = TRUE' if network.upper() == 'SYN' else f"s.show_network && ARRAY['{network.upper()}', 'Syndicated']"
        self.cur.execute(
            f"""
            SELECT s.show_id
            FROM shows s
            LEFT JOIN broadcast_log bl ON s.show_id = bl.show_id
            WHERE (bl.channel_id IS NULL OR bl.channel_id != %s)
            AND s.airdate_end <= CAST(%s AS DATE)
            AND %s = ANY (s.show_duration)
            AND s.show_is_syndicated = TRUE
            AND s.show_type != 'children'
            AND s.show_id NOT IN (
                SELECT show_id FROM schedule_template WHERE channel_id = %s AND replication_year = %s
            )
            ORDER BY 
                CASE 
                    WHEN %s = ANY(show_network) THEN random() * 0.5
                    WHEN 'Syndicated' = ANY(show_network) THEN random() * 1.5
                END
            LIMIT 1
            """, (channel_id, f"{year - SCHEDULE_RECURSION}-09-01", required_runtime, channel_id, year, network.upper()))
        result = self.cur.fetchone()
        return result[0] if result else None

    # Replace a show in the schedule if it has no remaining episodes
    def get_replacement_show(self, show_id, channel_id, year, required_runtime):
        query = """
        WITH EpisodeCount AS (
            SELECT e.show_id, COUNT(*) AS remaining_episodes
            FROM episodes e
            LEFT JOIN broadcast_log bl ON e.episode_id = bl.episode_id
            WHERE e.show_id = %s
            GROUP BY e.show_id
        ),
        ReplacementShow AS (
            SELECT s.show_id
            FROM shows s
            LEFT JOIN broadcast_log bl ON s.show_id = bl.show_id
            WHERE s.show_id NOT IN (SELECT show_id FROM broadcast_log WHERE channel_id = %s)
            AND s.airdate_end <= CAST(%s AS DATE)
            AND %s = ANY (s.show_duration)
            AND s.show_is_syndicated = TRUE
            AND s.show_type != 'children'
            ORDER BY random()
            LIMIT 1
        )
        UPDATE schedule_template st
        SET show_id = rs.show_id
        FROM ReplacementShow rs, EpisodeCount ec
        WHERE st.show_id = ec.show_id AND ec.remaining_episodes = 0;
        """
        self.cur.execute(query, (show_id, channel_id, f"{year - SCHEDULE_RECURSION}-09-01", required_runtime))
        self.db_connection.commit()

    # Calculate time differences between consecutive show time slots
    def calculate_time_differences(self, time_dict):
        time_format = '%H:%M:%S'
        # Sort the times in the schedule dictionary
        sorted_times = sorted(time_dict.keys(), key=lambda x: datetime.strptime(x, time_format))

        time_differences = {}
        # Calculate the difference between each consecutive time slot
        for i in range(1, len(sorted_times)):
            t1 = datetime.strptime(sorted_times[i - 1], time_format)
            t2 = datetime.strptime(sorted_times[i], time_format)
            time_difference = int((t2 - t1).total_seconds())
            time_differences[sorted_times[i - 1]] = time_difference

        # Handle the time difference for the last time slot
        last_time = datetime.strptime(sorted_times[-1], time_format)
        show_id = time_dict[sorted_times[-1]]
        if show_id is None:
            # If there's no show, calculate difference until midnight
            end_time = datetime.strptime("00:00:00", time_format) + timedelta(days=1)
            final_difference = int((end_time - last_time).total_seconds())
        elif show_id == MOVIE_ID:
            # Special case for movie, difference until 23:00
            end_time = datetime.strptime("23:00:00", time_format)
            final_difference = int((end_time - last_time).total_seconds())
        else:
            # Default case, use show duration for final time slot
            final_difference = self.get_show_duration(show_id)

        time_differences[sorted_times[-1]] = final_difference
        return time_differences

    # Process scheduled shows, organizing by air date and time
    def process_scheduled_shows(self, show_results, year):
        schedules = {}
        # Organize show results by air date and air time
        for show_id, air_time, air_date in show_results:
            air_time_str = air_time.strftime('%H:%M:%S')
            schedules.setdefault(air_date, {})[air_time_str] = show_id

        final_schedule = schedules[year]
        # Generate schedule recursively from past years
        for y in range(year - 1, year - SCHEDULE_RECURSION, -1):
            if y in schedules:  # Ensure year exists in the schedule
                for time, show_id in schedules[y].items():
                    # If the slot is empty in the final schedule, fill it
                    if time in final_schedule and final_schedule[time] is None:
                        final_schedule[time] = show_id

        # Sort the final schedule by time
        sorted_schedule = dict(sorted(final_schedule.items(), key=lambda item: datetime.strptime(item[0], '%H:%M:%S')))
        return sorted_schedule

    # Retrieve scheduled show IDs based on the year and day of the week (dow)
    def get_scheduled_shows_id(self, year, dow):
        query = """SELECT show_id, air_time, air_date FROM schedules 
                   WHERE air_date = %s AND network = %s AND day_of_week = %s;"""
        self.cur.execute(query, (year, self.network.upper(), calendar.day_name[dow].lower()))
        results = self.process_scheduled_shows(self.cur.fetchall(), year)
        return results

    # Retrieve show IDs for a specific network in a given year
    def get_network_shows_id(self, year):
        query = """SELECT show_id FROM shows 
                   WHERE %s BETWEEN EXTRACT(YEAR FROM shows.airdate_start) 
                   AND EXTRACT(YEAR FROM shows.airdate_end) 
                   AND show_type != 'children' 
                   AND show_network = %s;"""
        self.cur.execute(query, (year, self.network))
        return [record['show_id'] for record in self.cur.fetchall()]

#!/usr/bin/env python3
from psycopg.rows import dict_row
import calendar
from datetime import datetime, timedelta, time, date

# Constants
SCHEDULE_RECURSION = 3  # Number of past years to consider for schedule recursion
MOVIE_ID = 173  # Special ID for movies, used in time calculations


class Shows:
    def __init__(self, db, hostname):
        # Initialize database connection and parse hostname for network/channel details
        self.db_connection = db
        _, self.network, self.channel = hostname.split('-')
        # Create a cursor using DictCursor to fetch rows as dictionaries
        self.cur = db.cursor(row_factory=dict_row)

    # Fetch the duration of a specific show based on its ID and a minimum required duration
    def get_show_duration(self, show_id, duration=1500):
        query = """SELECT duration
                   FROM (SELECT unnest(show_duration) AS duration FROM shows WHERE show_id = %s) AS expanded
                   WHERE duration >= %s
                   ORDER BY duration ASC
                   LIMIT 1;"""
        self.cur.execute(query, (show_id, duration))
        return self.cur.fetchone()['duration']

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

    # Return the preferred genres for a given time-of-day and air date, biased by decade.
    def get_genre_bias_for_slot(self, start_time: time, air_date: date) -> list[str]:
        """
        Return the preferred genres for a given date/time.

        Defines genre bias tables for decades (1950s-1980s) and picks the table for the
        decade closest to `air_date.year`, then returns the genres for the hour range
        containing `start_time.hour`.
        """
        TIME_OF_DAY_GENRE_BIAS_50s = {
            range(6, 18): ['local', 'family'],
            range(18, 19): ['news', 'family'],
            range(19, 21): ['family', 'comedy'],
            range(21, 23): ['variety', 'drama'],
            range(23, 24): ['rerun', 'talk']
        }
        TIME_OF_DAY_GENRE_BIAS_60s = {
            range(6, 18): ['local', 'family', 'comedy'],
            range(18, 19): ['news', 'family', 'comedy'],
            range(19, 21): ['comedy', 'western', 'family', 'drama'],
            range(21, 23): ['drama', 'crime', 'variety', 'thriller'],
            range(23, 24): ['talk', 'rerun', 'variety']
        }
        TIME_OF_DAY_GENRE_BIAS_70s = {
            range(6, 18): ['local', 'family', 'comedy'],
            range(18, 19): ['news', 'family', 'comedy'],
            range(19, 21): ['comedy', 'family', 'drama', 'western'],
            range(21, 23): ['drama', 'crime', 'thriller', 'comedy'],
            range(23, 24): ['talk', 'rerun', 'drama']
        }
        TIME_OF_DAY_GENRE_BIAS_80s = {
            range(6, 18): ['local', 'family', 'comedy'],
            range(18, 19): ['news', 'family', 'comedy'],
            range(19, 21): ['comedy', 'family', 'drama', 'sitcom'],
            range(21, 23): ['drama', 'crime', 'thriller', 'action'],
            range(23, 24): ['talk', 'rerun', 'variety', 'drama']
        }
        genre_bias_by_decade = {
            1950: TIME_OF_DAY_GENRE_BIAS_50s,
            1960: TIME_OF_DAY_GENRE_BIAS_60s,
            1970: TIME_OF_DAY_GENRE_BIAS_70s,
            1980: TIME_OF_DAY_GENRE_BIAS_80s
        }

        decade = min(genre_bias_by_decade.keys(), key=lambda d: abs(d - air_date.year))
        bias_table = genre_bias_by_decade[decade]

        for hour_range, genres in bias_table.items():
            if start_time.hour in hour_range:
                return genres

        return ['comedy', 'drama', 'family']

    # Whether a show's declared slot durations include the given runtime (seconds).
    def show_supports_duration(self, show_id, required_runtime):
        self.cur.execute("SELECT %s = ANY(show_duration) AS fits FROM shows WHERE show_id = %s;",
                         (required_runtime, show_id))
        row = self.cur.fetchone()
        return bool(row and row['fits'])

    # Return the show's first listed genre, used for lead-in/lead-out adjacency scoring.
    def get_first_genre(self, show_id):
        self.cur.execute("SELECT show_genre FROM shows WHERE show_id = %s;", (show_id,))
        row = self.cur.fetchone()
        if not row or not row['show_genre']:
            return None
        return row['show_genre'].split(',')[0].strip()

    @staticmethod
    def _genre_order_clause(preferred_genres, prev_genre, params):
        """
        Build an ORDER BY prefix (and append the params it needs) that prioritizes:
        1. shows matching the daypart's preferred genres (e.g. decade/time-of-day bias)
        2. shows matching the previous slot's genre (lead-in/lead-out adjacency)
        ahead of whatever tiebreak the caller's query already uses.
        """
        clauses = []
        if preferred_genres:
            clauses.append("CASE WHEN string_to_array(s.show_genre, ',') && %s::text[] THEN 0 ELSE 1 END")
            params.append(preferred_genres)
        if prev_genre:
            clauses.append("CASE WHEN %s = ANY(string_to_array(s.show_genre, ',')) THEN 0 ELSE 1 END")
            params.append(prev_genre)
        return (",".join(clauses) + ",") if clauses else ""

    # Get an available show ID based on channel, year, required runtime, and network
    def get_available_show_id(self, channel_id, year, required_runtime, network, preferred_genres=None, prev_genre=None):
        # Condition for whether the show is syndicated or belongs to a specific network
        where_string = 's.show_is_syndicated = TRUE' if network.upper() == 'SYN' else f"s.show_network && ARRAY['{network.upper()}', 'Syndicated']"
        params = [channel_id, f"{year - SCHEDULE_RECURSION}-09-01", required_runtime, year]
        genre_order_by = self._genre_order_clause(preferred_genres, prev_genre, params)
        params.append(network.upper())

        self.cur.execute(
            f"""
            SELECT s.show_id
            FROM shows s
            LEFT JOIN broadcast_log bl ON s.show_id = bl.show_id
            WHERE (bl.channel_id IS NULL OR bl.channel_id != %s)
            AND s.airdate_end <= CAST(%s AS DATE)
            AND %s = ANY (s.show_duration)
            AND {where_string}
            AND s.show_type != 'children'
            AND s.show_id NOT IN (
                -- Exclusive across ALL channels for this replication_year, not just this one --
                -- a syndicated show is licensed to one station in a market, not several at once.
                SELECT show_id FROM schedule_template WHERE replication_year = %s
            )
            ORDER BY
                {genre_order_by}
                CASE
                    WHEN %s = ANY(show_network) THEN random() * 0.5
                    WHEN 'Syndicated' = ANY(show_network) THEN random() * 1.5
                END
            LIMIT 1
            """, params)
        result = self.cur.fetchone()
        return result['show_id'] if result else None

    # Replace a show in the schedule if it has no remaining episodes
    def get_replacement_show(self, show_id, channel_id, year, required_runtime, preferred_genres=None, prev_genre=None):
        params = [channel_id, show_id, channel_id, f"{year - SCHEDULE_RECURSION}-09-01", required_runtime]
        genre_order_by = self._genre_order_clause(preferred_genres, prev_genre, params)
        query = f"""
        WITH EpisodeCount AS (
            SELECT e.show_id, COUNT(*) FILTER (WHERE bl.episode_id IS NULL) AS remaining_episodes
            FROM episodes e
            LEFT JOIN broadcast_log bl ON e.episode_id = bl.episode_id AND bl.channel_id = %s
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
            ORDER BY {genre_order_by} random()
            LIMIT 1
        )
        UPDATE schedule_template st
        SET show_id = rs.show_id
        FROM ReplacementShow rs, EpisodeCount ec
        WHERE st.show_id = ec.show_id AND ec.remaining_episodes = 0;
        """
        self.cur.execute(query, params)
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
    def process_scheduled_shows(self, show_results, year, ended_show_ids=frozenset()):
        schedules = {}
        # Organize show results by air date and air time
        for row in show_results:
            show_id, air_time, air_date = row['show_id'], row['air_time'], row['air_date']
            air_time_str = air_time.strftime('%H:%M:%S')
            schedules.setdefault(air_date, {})[air_time_str] = show_id

        final_schedule = schedules[year]
        # Generate schedule recursively from past years
        for y in range(year - 1, year - SCHEDULE_RECURSION, -1):
            if y in schedules:  # Ensure year exists in the schedule
                for time, show_id in schedules[y].items():
                    # If the slot is empty in the final schedule, fill it -- but never resurrect a
                    # show that had already ended by the target year.
                    if time in final_schedule and final_schedule[time] is None and show_id not in ended_show_ids:
                        final_schedule[time] = show_id

        # Sort the final schedule by time
        sorted_schedule = dict(sorted(final_schedule.items(), key=lambda item: datetime.strptime(item[0], '%H:%M:%S')))
        return sorted_schedule


    # Retrieve scheduled show IDs based on the year and day of the week (dow)
    def get_scheduled_shows_id(self, year, dow):
        query = """SELECT show_id, air_time, air_date FROM schedules
                   WHERE air_date = %s AND network = %s AND day_of_week = %s;"""
        self.cur.execute(query, (year, self.network.upper(), calendar.day_name[dow].lower()))
        rows = self.cur.fetchall()

        self.cur.execute(
            """SELECT show_id FROM shows
               WHERE show_id = ANY(%s) AND airdate_end IS NOT NULL AND EXTRACT(YEAR FROM airdate_end) < %s;""",
            ([r['show_id'] for r in rows], year))
        ended_show_ids = frozenset(r['show_id'] for r in self.cur.fetchall())

        results = self.process_scheduled_shows(rows, year, ended_show_ids)
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

#!/usr/bin/env python3
from psycopg.rows import dict_row
import calendar
from datetime import datetime, timedelta, time, date

# Constants
SCHEDULE_RECURSION = 3  # Number of past years to consider for schedule recursion
MOVIE_ID = 173  # Special ID for movies, used in time calculations
UNEXPLAINED_GAP_SECONDS = 1800  # a gap this much longer than an anchored show can run is other programming, not the show


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
        row = self.cur.fetchone()
        if row is None:
            # The episode runs longer than every listed length (e.g. a 90-minute pilot of a one-hour
            # show): use the show's longest listed length so the playlist still builds, with no
            # commercial time left to fill.
            self.cur.execute("SELECT MAX(d) AS duration FROM (SELECT unnest(show_duration) AS d FROM shows WHERE show_id = %s) x",
                             (show_id,))
            row = self.cur.fetchone()
        return row['duration']

    def get_longest_show_duration(self, show_id):
        self.cur.execute("SELECT MAX(d) AS duration FROM (SELECT unnest(show_duration) AS d FROM shows WHERE show_id = %s) x",
                         (show_id,))
        row = self.cur.fetchone()
        return row['duration'] if row else None

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

    # Replace a show in the schedule if it has no remaining episodes. `show_id` here is the
    # concrete show that just aired an episode -- for an umbrella slot (e.g. NBC Mystery Movie)
    # that's the component series (e.g. Columbo), not the umbrella itself, since get_next_episode
    # returns the real component show_id. `umbrella`/`target_show` walk back to whichever show_id
    # actually sits in schedule_template (the umbrella, if this show is one of its components) so
    # the UPDATE below matches the right row, and remaining episodes are counted across every
    # component so the umbrella is only rotated out once ALL of its component series are exhausted.
    def get_replacement_show(self, show_id, channel_id, year, required_runtime, preferred_genres=None, prev_genre=None):
        params = [show_id, show_id, channel_id, channel_id, f"{year - SCHEDULE_RECURSION}-09-01", required_runtime]
        genre_order_by = self._genre_order_clause(preferred_genres, prev_genre, params)
        query = f"""
        WITH umbrella AS (
            SELECT ss.show_id AS umbrella_id FROM sub_shows ss WHERE %s = ANY(ss.sub_show_ids) LIMIT 1
        ),
        target_show AS (
            SELECT COALESCE((SELECT umbrella_id FROM umbrella), %s) AS show_id
        ),
        component_ids AS (
            SELECT unnest(ss.sub_show_ids) AS show_id
            FROM sub_shows ss, target_show ts WHERE ss.show_id = ts.show_id
            UNION ALL
            SELECT ts.show_id FROM target_show ts
            WHERE NOT EXISTS (SELECT 1 FROM sub_shows ss WHERE ss.show_id = ts.show_id)
        ),
        EpisodeCount AS (
            SELECT COUNT(*) FILTER (WHERE bl.episode_id IS NULL) AS remaining_episodes
            FROM episodes e
            JOIN component_ids ci ON ci.show_id = e.show_id
            LEFT JOIN broadcast_log bl ON e.episode_id = bl.episode_id AND bl.channel_id = %s
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
        FROM ReplacementShow rs, EpisodeCount ec, target_show ts
        WHERE st.show_id = ts.show_id AND ec.remaining_episodes = 0;
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
            # An anchored show only holds its own length: when the gap to the next anchor is more than
            # UNEXPLAINED_GAP_SECONDS longer than the longest length it can run, the leftover is other
            # programming, which generate_schedule fills. Without this a 30-minute show anchored at 15:00
            # with the next anchor at 20:00 got a 5-hour slot (and 11 packed episodes). Movies and shows
            # with no linked show_id keep the full gap, since their real length isn't known here.
            show_id = time_dict[sorted_times[i - 1]]
            if show_id is not None and show_id != MOVIE_ID:
                longest = self.get_longest_show_duration(show_id)
                if longest and time_difference > longest + UNEXPLAINED_GAP_SECONDS:
                    time_difference = longest
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

    # Process scheduled shows, organizing by air time.
    def process_scheduled_shows(self, show_results):
        final_schedule = {}
        for row in show_results:
            air_time_str = row['air_time'].strftime('%H:%M:%S')
            final_schedule[air_time_str] = row['show_id']

        sorted_schedule = dict(sorted(final_schedule.items(), key=lambda item: datetime.strptime(item[0], '%H:%M:%S')))
        return sorted_schedule


    # Retrieve scheduled show IDs for a specific calendar date and day of the week (dow).
    def get_scheduled_shows_id(self, sim_date, dow):
        # Matches on `schedules.effective_start_date`/`effective_end_date` (real date ranges,
        # added so mid-season show replacements can be represented precisely) rather than the
        # legacy year-only `air_date` int -- see migrate_schedule_date_ranges.py. Every row
        # defaults to (Jan 1, air_date)-(Dec 31, air_date) unless specifically researched and
        # narrowed, so this is backward compatible for any row that hasn't needed splitting.
        #
        # This replaces a previous version of this method that queried by exact year and fed
        # multiple years' rows into a "recurse into prior years for empty slots" loop in
        # process_scheduled_shows -- that loop was actually unreachable dead code (the query only
        # ever fetched one year's rows, so its recursion could never find a second year to fall
        # back to), so it was removed rather than ported forward. If you want real cross-year
        # gap-filling for slots with no exact-date match, that would need to be (re)built here as
        # a deliberate feature, not implied by date-range matching alone.
        #
        # ORDER BY range width DESC, schedule_id: multiple rows can legitimately match the same
        # date/time (a season row narrowed to Sep-Apr overlaps the next year's still-default
        # Jan-Dec row for Jan-Apr). process_scheduled_shows keeps the LAST row per air_time, so
        # sorting widest-first makes the narrowest (most specific, researched) range win; ties
        # fall back to schedule_id so the result stays reproducible.
        query = """SELECT show_id, air_time FROM schedules
                   WHERE %s BETWEEN effective_start_date AND effective_end_date
                   AND network = %s AND day_of_week = %s
                   ORDER BY (effective_end_date - effective_start_date) DESC, schedule_id;"""
        self.cur.execute(query, (sim_date, self.network.upper(), calendar.day_name[dow].lower()))
        rows = self.cur.fetchall()

        return self.process_scheduled_shows(rows)

    # Retrieve show IDs for a specific network in a given year
    def get_network_shows_id(self, year):
        query = """SELECT show_id FROM shows 
                   WHERE %s BETWEEN EXTRACT(YEAR FROM shows.airdate_start) 
                   AND EXTRACT(YEAR FROM shows.airdate_end) 
                   AND show_type != 'children' 
                   AND show_network = %s;"""
        self.cur.execute(query, (year, self.network))
        return [record['show_id'] for record in self.cur.fetchall()]

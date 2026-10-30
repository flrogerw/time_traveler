#!/usr/bin/env python3
import logging
from collections import defaultdict

from psycopg.rows import dict_row
from psycopg import sql
from datetime import datetime, timedelta
from playlists.classes.Shows import Shows
from playlists.classes.Schedules import Schedules
from playlists.classes.Movies import Movies
from playlists.classes.SyndicatedScheduler import daypart_column_for

# Configure logging to capture important events and errors
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Short-form shows (cartoon shorts like Bullwinkle, Road Runner, Wally Gator) have episodes far
# shorter than their slot: pack several into one block instead of airing one 5-minute short in a
# 30-minute slot. A show is "shorts" when its typical episode is under this fraction of the slot;
# content is packed up to PACK_CONTENT_FRACTION of the slot, leaving the rest for commercials.
SHORTS_FRACTION_OF_SLOT = 0.6
PACK_CONTENT_FRACTION = 0.8
MAX_PACKED_EPISODES = 12

# Episode-level fit: an episode must run no longer than its slot (plus a small tolerance) to be the
# preferred pick; see get_next_episode.
EPISODE_FITS_SLOT_SQL = "AND (e.end_point - e.start_point) <= EXTRACT(EPOCH FROM st.runtime) * 1.05"

# Interstitial shows (mapped_genre 'interstitial', e.g. School House Rock) air between programs. They
# were an ABC property, so besides every syndicated channel they air on these networks' channels.
INTERSTITIAL_NETWORKS = {"ABC"}


def channel_airs_interstitials(channel_type, network_name):
    """Whether a channel plays interstitials: all syndicated channels, plus INTERSTITIAL_NETWORKS."""
    return channel_type == "syndicated" or (network_name or "").upper() in INTERSTITIAL_NETWORKS


# Class to handle episode-related database operations
class Episodes:
    def __init__(self, db, hostname, year):
        # Initialize database connection and related classes for shows, movies, and schedules
        self.db_connection = db
        self.shows = Shows(self.db_connection, hostname)
        self.movies = Movies(self.db_connection, year)
        self.schedules = Schedules(self.db_connection, hostname)
        self.cur = db.cursor(row_factory=dict_row)  # Cursor to execute queries

    def get_episodes_for_holiday(self, holiday):
        try:
            query = sql.SQL("""SELECT *, NULL AS time_slot, end_point - start_point as duration
                                FROM episodes
                                WHERE episode_season = %s
                                ORDER BY show_id, show_season_number, episode_number""")

            # Execute the query and retrieve the next episode
            self.cur.execute(query, (holiday,))
            formatted_record = [{**dict(record), 'type': 'episode'} for record in self.cur.fetchall()]

        except Exception as e:
            # Log any errors encountered during the query
            logging.error(f"Could not get Next Episode: {e}")
        else:
            return formatted_record


    def get_holiday_episodes_in_use(self, holiday, replication_year):
        """
        (episode_ids, show_ids) of `holiday` episodes already claimed on ANY channel: pinned in
        schedule_overrides (not yet consumed) or already logged in broadcast_log for this
        replication year. Holiday pinning runs once per channel, so without this each channel
        independently picks the same best-fit episode.
        """
        try:
            self.cur.execute(
                """SELECT e.episode_id, e.show_id FROM episodes e
                   WHERE e.episode_season = %s
                     AND (e.episode_id IN (SELECT episode_id FROM schedule_overrides)
                          OR e.episode_id IN (SELECT episode_id FROM broadcast_log
                                              WHERE replication_year = %s));""",
                (holiday, replication_year))
            rows = self.cur.fetchall()
        except Exception as e:
            logging.error(f"Could not get holiday episodes in use: {e}")
            self.db_connection.rollback()
            return set(), set()
        return {r['episode_id'] for r in rows}, {r['show_id'] for r in rows}

    def manage_time_slot_expansion(self, slot, dow, final_episodes, network, air_date=None, preferred_show_id=None):
        """
        Handle the expansion of time slots into 30-minute segments if no suitable show is found
        for the given slot. Updates schedules and inserts new time slots.

        When `preferred_show_id` (the show that didn't fit the original, larger slot) supports a
        30-minute runtime, it's reused across the split sub-slots -- a back-to-back double episode
        reads better than pairing two unrelated shows -- falling back to independent selection per
        sub-slot once that show's unaired episodes run out.
        """
        # Delete the current schedule row for the slot
        self.schedules.delete_row(slot['id'])

        # Convert the slot duration to total seconds
        time_obj = datetime.strptime(slot['duration'], '%H:%M:%S')
        total_seconds = int(timedelta(hours=time_obj.hour, minutes=time_obj.minute,
                                      seconds=time_obj.second).total_seconds())

        # Determine the number of 30-minute slots needed to fill the original time slot
        num_loops = int(total_seconds // 1800)
        slot['duration'] = '00:30:00'  # Update duration to 30 minutes
        duration_delta = timedelta(minutes=30)  # Time delta of 30 minutes
        new_end_time = datetime.strptime(slot['start_time'], '%H:%M:%S')

        reuse_show_id = None
        if preferred_show_id and self.shows.show_supports_duration(preferred_show_id, 1800):
            reuse_show_id = preferred_show_id

        # Loop to insert new time slots in 30-minute increments
        for _ in range(num_loops):
            self.schedules.insert_time_slot_schedule(slot['schedule_id'], slot['channel_id'],
                                                     slot['start_time'], duration_delta, dow)
            new_end_time += duration_delta
            slot['end_time'] = new_end_time.strftime('%H:%M:%S')  # Update the end time of the slot
            self.schedules.insert_schedule_template(slot, dow, network, reuse_show_id)
            episode = self.get_next_episode(slot, dow, air_date=air_date)  # Get next episode for the new slot
            if episode is None and reuse_show_id is not None:
                # The reused show just ran out of unaired episodes -- drop back to a fresh pick
                # for this and the remaining sub-slots instead of failing the whole expansion.
                reuse_show_id = None
                self.schedules.insert_schedule_template(slot, dow, network, None)
                episode = self.get_next_episode(slot, dow, air_date=air_date)

            if episode:
                final_episodes.append(episode)
                slot['start_time'] = slot['end_time']  # Update start time for the next iteration
            else:
                raise ValueError(f"Could not generate an episode for slot {slot} with day of week {dow}")

    def get_manual_episodes(self, time_slots: list):
        final_episodes = []

        # Build mapping: { "episodes_123": original_slot_time }
        slot_time_map = {
            slot["episode_id"]: (slot["start_time"], slot["duration"], slot['replication_year'])
            for slot in time_slots
        }

        # Group ids by table: { "episodes": [123, 124], "specials": [5] }
        grouped = defaultdict(list)
        for slot in time_slots:
            table_name, id_str = slot["episode_id"].split("_")
            grouped[table_name].append(int(id_str))

        try:
            # Process each table only once
            for table_name, ids in grouped.items():

                # Normalize table name
                table_name = "episodes" if table_name == "shows" else table_name
                base_name = table_name.rstrip("s")
                id_col = f"{base_name}_id"

                # Safe SQL
                query = sql.SQL("""
                    SELECT *, (end_point - start_point) AS duration
                    FROM {table}
                    WHERE {id_col} = ANY(%s)
                """).format(
                    table=sql.Identifier(table_name),
                    id_col=sql.Identifier(id_col),
                )

                self.cur.execute(query, (ids,))
                rows = self.cur.fetchall()

                for row in rows:
                    row_dict = dict(row)
                    table_name = 'shows' if table_name == 'episodes' else table_name
                    eid = f"{table_name}_{row_dict[id_col]}"

                    # Use ORIGINAL time_slot from input
                    time_slot, time_slot_duration, replication_year  = slot_time_map.get(eid)

                    t = datetime.strptime(time_slot_duration, "%H:%M:%S")
                    seconds = t.hour * 3600 + t.minute * 60 + t.second

                    final_episodes.append({
                        **row_dict,
                        "type": base_name,
                        "time_slot": time_slot,
                        "slot_duration": seconds,
                        "replication_year": replication_year
                    })

        except Exception as e:
            logging.error(f"Could not get episodes: {e}")
            return []

        return final_episodes


    def get_episodes_for_slot(self, slot, channel_id, dow, year, network, current_schedule=None, air_date=None, prev_genre=None):
        """
        Retrieve and manage episodes for a given time slot.
        This function looks for a suitable episode or expands the slot if needed.
        """
        if current_schedule is None:
            current_schedule = {}

        try:
            final_episodes = []
            time_obj = datetime.strptime(slot['duration'], '%H:%M:%S')
            total_seconds = int(timedelta(hours=time_obj.hour, minutes=time_obj.minute,
                                          seconds=time_obj.second).total_seconds())

            preferred_genres = None
            if air_date is not None:
                slot_time = datetime.strptime(slot['start_time'], '%H:%M:%S').time()
                preferred_genres = self.shows.get_genre_bias_for_slot(slot_time, air_date)

            # Attempt to get the next episode for the time slot
            episode = self.get_next_episode(slot, dow, air_date=air_date)
            if episode:
                final_episodes.append(episode)
                self._pack_short_episodes(slot, dow, final_episodes, total_seconds, air_date)
                # Rotate in a new show once its episodes are exhausted -- unless this episode is a
                # non-final part of a multi-part story with its continuation still unaired, in which
                # case rotating the show out now would orphan the arc mid-story.
                if not episode.get('has_pending_continuation'):
                    self.shows.get_replacement_show(episode['show_id'], channel_id, year, total_seconds,
                                                    preferred_genres, prev_genre)
            else:
                # If no episode is found, check for a scheduled show or attempt to fill the slot
                show_id = current_schedule.get(slot['start_time'])
                if not show_id:
                    show_id = self.shows.get_available_show_id(channel_id, year, total_seconds, network,
                                                                preferred_genres, prev_genre)

                self.schedules.insert_schedule_template(slot, dow, network, show_id)
                episode = self.get_next_episode(slot, dow, air_date=air_date)  # Retry getting the next episode
                if episode:
                    final_episodes.append(episode)
                    self._pack_short_episodes(slot, dow, final_episodes, total_seconds, air_date)
                elif show_id == 173 or total_seconds > 3600:
                    # If it's a movie slot (show_id 173) or a length greater than one hour, insert a random movie
                    movie = self.movies.get_random_movie(total_seconds, slot)
                    if movie:
                        final_episodes.append(movie)
                    else:
                        # No unshown movie fits this channel/duration -- fall back to filling
                        # the slot with shows instead of leaving it empty.
                        self.manage_time_slot_expansion(slot, dow, final_episodes, network, air_date=air_date,
                                                        preferred_show_id=show_id)
                else:
                    # If no match, try to expand the slot into 30-minute shows
                    self.manage_time_slot_expansion(slot, dow, final_episodes, network, air_date=air_date,
                                                    preferred_show_id=show_id)

            return final_episodes
        except Exception as e:
            # Log any errors that occur during slot processing
            logging.error(f"Error processing slot {slot}: {e}")
            raise


    def _episode_seconds(self, episode):
        return float(episode['end_point'] - episode['start_point'])

    def _pack_short_episodes(self, slot, dow, final_episodes, total_seconds, air_date=None):
        """If the slot's first episode is a short (see SHORTS_FRACTION_OF_SLOT), pull more of the
        same show's next unaired episodes into the slot until about PACK_CONTENT_FRACTION of it is
        content, then tag every packed episode (pack_id/pack_index/pack_size/pack_slot_seconds) so
        the playlist fills the block's commercial time ONCE, with a pod between the shorts, instead
        of treating each short as a full program. No-op for ordinary shows.

        The stop test uses the show's typical episode length, not the next episode's, so no episode
        is consumed (and logged as aired) unless it will fit."""
        if not final_episodes or final_episodes[0].get('type') != 'episode':
            return
        first = final_episodes[0]
        self.cur.execute(
            "SELECT AVG(end_point - start_point) AS typical FROM episodes WHERE show_id = %s", (first['show_id'],))
        row = self.cur.fetchone()
        typical = float(row['typical']) if row and row['typical'] is not None else 0.0
        if typical <= 0 or typical >= SHORTS_FRACTION_OF_SLOT * total_seconds:
            return

        accumulated = self._episode_seconds(first)
        while (len(final_episodes) < MAX_PACKED_EPISODES
               and accumulated + typical <= PACK_CONTENT_FRACTION * total_seconds):
            nxt = self.get_next_episode(slot, dow, air_date=air_date)
            if not nxt or nxt['show_id'] != first['show_id']:
                break
            final_episodes.append(nxt)
            accumulated += self._episode_seconds(nxt)

        if len(final_episodes) > 1:
            for index, episode in enumerate(final_episodes):
                episode['pack_id'] = slot['start_time']
                episode['pack_index'] = index
                episode['pack_size'] = len(final_episodes)
                episode['pack_slot_seconds'] = total_seconds

    def get_interstitial_show_id(self, year, dow, start_time):
        """show_id of an interstitial show (mapped_genre 'interstitial') eligible to air in the break
        at `start_time` on `dow` in `year`, or None. Interstitials never take a slot of their own --
        no fill rule asks for that category -- they air between programs, in the daypart blocks and
        years their eligibility row allows."""
        column = daypart_column_for(start_time, dow)
        if column is None:
            return None
        self.cur.execute(
            f"""
            SELECT show_id FROM show_syndication_eligibility
            WHERE mapped_genre = 'interstitial' AND {column} = TRUE
              AND eligible_year_start IS NOT NULL AND eligible_year_end IS NOT NULL
              AND %s BETWEEN eligible_year_start AND eligible_year_end
            ORDER BY show_id LIMIT 1
            """,
            (year,),
        )
        row = self.cur.fetchone()
        return row["show_id"] if row else None

    def get_next_interstitial(self, show_id, channel_id, replication_year, time_slot):
        """
        Next episode of an interstitial show (e.g. School House Rock), which airs in the breaks
        BETWEEN regular programs instead of owning a slot. Picks the lowest unaired
        (season, episode) for this channel; once every episode has aired it reuses the least
        recently aired one, since a short interstitial library repeats many times over a run.
        Logs the pick to broadcast_log against `time_slot` (the program it follows), so the
        playlist's time_slot sort keeps it right after that program. Returns None if the show
        has no episodes.
        """
        try:
            self.cur.execute(
                """
                SELECT e.episode_id
                FROM episodes e
                LEFT JOIN (
                    SELECT episode_id, MAX(date_played) AS last_played
                    FROM broadcast_log WHERE channel_id = %s GROUP BY episode_id
                ) bl ON bl.episode_id = e.episode_id
                WHERE e.show_id = %s
                ORDER BY (bl.last_played IS NOT NULL), bl.last_played ASC, e.show_season_number, e.episode_number
                LIMIT 1;""",
                (channel_id, show_id))
            row = self.cur.fetchone()
            if row is None:
                return None

            self.cur.execute(
                """INSERT INTO broadcast_log (channel_id, show_id, episode_id, date_played, time_slot, replication_year)
                   VALUES (%s, %s, %s, current_date, %s::time, %s)
                   ON CONFLICT (channel_id, show_id, episode_id, date_played) DO NOTHING;""",
                (channel_id, show_id, row['episode_id'], time_slot, replication_year))

            self.cur.execute("SELECT e.* FROM episodes e WHERE e.episode_id = %s", (row['episode_id'],))
            episode = {**dict(self.cur.fetchone()), 'type': 'episode', 'has_pending_continuation': False,
                       'interstitial': True,
                       'time_slot': datetime.strptime(time_slot, '%H:%M:%S').time() if isinstance(time_slot, str) else time_slot,
                       'replication_year': replication_year}
            return episode
        except Exception as e:
            logging.error(f"Could not get next interstitial: {e}")
        finally:
            self.db_connection.commit()

    @staticmethod
    def get_true_duration(db, episode_id):
        """
        Get the true duration of an episode by subtracting commercial breaks from the runtime.
        Reads start/end points from the episodes table (episode_durations is not used).
        """
        db.execute("""SELECT (e.end_point - e.start_point) - COALESCE(SUM(cb.resume_point - cb.break_point), 0) AS final_duration
                          FROM public.episodes e
                          LEFT JOIN public.commercial_breaks cb ON e.episode_id = cb.media_id
                          WHERE e.episode_id = %s
                          GROUP BY e.end_point, e.start_point;""", (episode_id,))
        return db.fetchone()['final_duration']


    def _handle_multipart_continuation(self, episode, time_slot, air_date):
        """
        If `episode` is a non-final part of a multi-part story, reserve this slot's next
        calendar-day occurrence for the continuation (via schedule_overrides) so it airs the
        very next night regardless of the show's normal recurring cadence.

        Returns True if a continuation is still unaired (whether or not it could be scheduled),
        so callers can avoid rotating the show out of its slot mid-arc.
        """
        if not episode.get('episode_is_multipart'):
            return False

        # Match on the exact next episode_number (not just the next part-index), since a show can
        # have multiple multi-part arcs whose "part 2"s otherwise share the same part-index value.
        self.cur.execute(
            """SELECT episode_id FROM episodes
               WHERE show_id = %s AND episode_number = %s AND episode_is_multipart = %s
               AND episode_id NOT IN (SELECT episode_id FROM broadcast_log)
               LIMIT 1;""",
            (episode['show_id'], episode['episode_number'] + 1, episode['episode_is_multipart'] + 1))
        continuation = self.cur.fetchone()
        if not continuation:
            return False

        if air_date is not None:
            next_day = air_date + timedelta(days=1)
            self.cur.execute(
                """INSERT INTO schedule_overrides (channel_id, air_date, time_slot, episode_id)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (channel_id, air_date, time_slot) DO NOTHING;""",
                (time_slot['channel_id'], next_day, time_slot['start_time'], continuation['episode_id']))
        return True

    def get_next_episode(self, time_slot, day_of_week, air_date=None):
        """
        Retrieve the next episode that hasn't been broadcast for the given time slot and day of the week.
        Inserts the episode into the broadcast log to track it as played.

        If a schedule_overrides row exists for this channel/slot/air_date (e.g. a multi-part
        continuation reserved by a prior call), it takes priority over the normal season/episode
        sequential lookup.
        """
        try:
            episode_row = None

            if air_date is not None:
                self.cur.execute(
                    """SELECT episode_id FROM schedule_overrides
                       WHERE channel_id = %s AND air_date = %s AND time_slot = %s;""",
                    (time_slot['channel_id'], air_date, time_slot['start_time']))
                override = self.cur.fetchone()
                if override:
                    self.cur.execute(
                        """INSERT INTO broadcast_log (channel_id, show_id, episode_id, date_played, time_slot, replication_year)
                           SELECT %s, e.show_id, e.episode_id, current_date, %s::time, %s
                           FROM episodes e WHERE e.episode_id = %s
                           RETURNING episode_id;""",
                        (time_slot['channel_id'], time_slot['start_time'], time_slot['schedule_id'], override['episode_id']))
                    episode_row = self.cur.fetchone()
                    self.cur.execute(
                        """DELETE FROM schedule_overrides WHERE channel_id = %s AND air_date = %s AND time_slot = %s;""",
                        (time_slot['channel_id'], air_date, time_slot['start_time']))

            if episode_row is None:
                # An "umbrella" show (e.g. NBC Mystery Movie) has a row in sub_shows mapping its
                # show_id to the component series that actually rotated through that slot (e.g.
                # Columbo, McCloud, Banacek, McMillan & Wife). component_shows resolves st.show_id
                # to those component show_ids when it's an umbrella, or to itself otherwise, so an
                # ordinary show's selection is completely unaffected.
                #
                # For an umbrella slot, ordering by the real episode_airdate (already ingested per
                # episode) reconstructs the actual historical rotation across component series
                # directly from data, instead of a hand-maintained synthetic rotation map -- for a
                # non-umbrella show that column is NULL so it falls through to the normal
                # season/episode ordering, unchanged.
                query = sql.SQL("""
                                WITH NextEpisode AS (
                                    SELECT
                                        st.channel_id,
                                        st.time_slot,
                                        e.show_id,
                                        e.episode_id,
                                        st.runtime,
                                        st.replication_year,
                                        st.days_of_week
                                    FROM
                                        schedule_template st
                                    JOIN LATERAL (
                                        SELECT unnest(ss.sub_show_ids) AS show_id
                                        FROM sub_shows ss WHERE ss.show_id = st.show_id
                                        UNION ALL
                                        SELECT st.show_id WHERE NOT EXISTS (
                                            SELECT 1 FROM sub_shows ss WHERE ss.show_id = st.show_id
                                        )
                                    ) cs ON TRUE
                                    JOIN episodes e ON e.show_id = cs.show_id
                                    LEFT JOIN broadcast_log bl ON bl.episode_id = e.episode_id
                                                               AND bl.channel_id = st.channel_id
                                    WHERE
                                        bl.episode_id IS NULL
                                        AND st.channel_id = %s
                                        AND st.replication_year = %s
                                        AND st.time_slot::text = %s
                                        AND %s = ANY(st.days_of_week)
                                        {fit}
                                    ORDER BY e.episode_airdate ASC NULLS LAST, e.show_season_number, e.episode_number
                                    LIMIT 1
                                )
                                INSERT INTO broadcast_log (channel_id, show_id, episode_id, date_played, time_slot, replication_year)
                                SELECT
                                    ne.channel_id,
                                    ne.show_id,
                                    ne.episode_id,
                                    current_date,
                                    ne.time_slot::time,
                                    ne.replication_year
                                FROM NextEpisode ne RETURNING episode_id;""")

                # Prefer the next unaired episode that FITS the slot (a 64-minute pilot of a one-hour
                # show doesn't belong in its 60-minute slot); if none fits, fall back to the plain
                # next unaired episode so a show is never starved by this preference.
                for fit_clause in (EPISODE_FITS_SLOT_SQL, ""):
                    self.cur.execute(query.format(fit=sql.SQL(fit_clause)),
                                     (time_slot['channel_id'], time_slot['schedule_id'], time_slot['start_time'], day_of_week))
                    episode_row = self.cur.fetchone()
                    if episode_row is not None:
                        break

            if episode_row is None:
                return None

            # Query to fetch detailed episode information
            query = sql.SQL("""SELECT e.*, bl.time_slot, bl.replication_year FROM episodes e
                                    LEFT JOIN broadcast_log bl ON e.episode_id = bl.episode_id
                                    WHERE e.episode_id = %s""")
            self.cur.execute(query, (episode_row['episode_id'],))
            formatted_record = [{**dict(record), 'type': 'episode'} for record in self.cur.fetchall()]
            episode = formatted_record[0]

            episode['has_pending_continuation'] = self._handle_multipart_continuation(episode, time_slot, air_date)
            return episode
        except Exception as e:
            # Log any errors encountered during the query
            logging.error(f"Could not get Next Episode: {e}")
        finally:
            self.db_connection.commit()  # Ensure the transaction is committed to the database

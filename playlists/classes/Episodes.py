#!/usr/bin/env python3
import logging
from psycopg2.extras import DictCursor
from psycopg2 import sql
from datetime import datetime, timedelta
from playlists.classes.Shows import Shows
from playlists.classes.Schedules import Schedules
from playlists.classes.Movies import Movies

# Configure logging to capture important events and errors
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Class to handle episode-related database operations
class Episodes:
    def __init__(self, db, hostname, year):
        # Initialize database connection and related classes for shows, movies, and schedules
        self.db_connection = db
        self.shows = Shows(self.db_connection, hostname)
        self.movies = Movies(self.db_connection, year)
        self.schedules = Schedules(self.db_connection, hostname)
        self.cur = db.cursor(cursor_factory=DictCursor)  # Cursor to execute queries

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


    def manage_time_slot_expansion(self, slot, dow, final_episodes, network):
        """
        Handle the expansion of time slots into 30-minute segments if no suitable show is found
        for the given slot. Updates schedules and inserts new time slots.
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

        # Loop to insert new time slots in 30-minute increments
        for _ in range(num_loops):
            self.schedules.insert_time_slot_schedule(slot['schedule_id'], slot['channel_id'],
                                                     slot['start_time'], duration_delta, dow)
            new_end_time += duration_delta
            slot['end_time'] = new_end_time.strftime('%H:%M:%S')  # Update the end time of the slot
            self.schedules.insert_schedule_template(slot, dow, network)
            episode = self.get_next_episode(slot, dow)  # Get next episode for the new slot
            if episode:
                final_episodes.append(episode)
                slot['start_time'] = slot['end_time']  # Update start time for the next iteration
            else:
                raise ValueError(f"Could not generate an episode for slot {slot} with day of week {dow}")

    def get_episodes_for_slot(self, slot, channel_id, dow, year, network, current_schedule=None):
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

            # Attempt to get the next episode for the time slot
            episode = self.get_next_episode(slot, dow)
            if episode:
                final_episodes.append(episode)
                # Check if the show needs to be replaced and rotate a new show if it's the last episode
                self.shows.get_replacement_show(episode['show_id'], channel_id, year, total_seconds)
            else:
                # If no episode is found, check for a scheduled show or attempt to fill the slot
                show_id = current_schedule.get(slot['start_time'])
                self.schedules.insert_schedule_template(slot, dow, network, show_id)

                episode = self.get_next_episode(slot, dow)  # Retry getting the next episode
                if episode:
                    final_episodes.append(episode)
                elif show_id == 173 or total_seconds > 3600:
                    # If it's a movie slot (show_id 173) or a length greater than one hour, insert a random movie
                    movie = self.movies.get_random_movie(total_seconds, slot)
                    final_episodes.append(movie)
                else:
                    # If no match, try to expand the slot into 30-minute shows
                    self.manage_time_slot_expansion(slot, dow, final_episodes, network)

            return final_episodes
        except Exception as e:
            # Log any errors that occur during slot processing
            logging.error(f"Error processing slot {slot}: {e}")
            raise


    @staticmethod
    def get_true_duration(db, episode_id):
        """
        Get the true duration of an episode by subtracting commercial breaks from the runtime.
        """
        db.execute("""SELECT (end_point - start_point) - COALESCE(SUM(resume_point - break_point), 0) AS final_duration
                          FROM public.episode_durations
                          LEFT JOIN public.commercial_breaks 
                          ON episode_durations.episode_id = commercial_breaks.media_id 
                          WHERE episode_durations.episode_id = %s
                          GROUP BY end_point, start_point;""", (episode_id,))
        return db.fetchone()[0]


    def get_next_episode(self, time_slot, day_of_week):
        """
        Retrieve the next episode that hasn't been broadcast for the given time slot and day of the week.
        Inserts the episode into the broadcast log to track it as played.
        """
        try:
            query = sql.SQL("""
                            WITH NextEpisode AS (
                                SELECT
                                    st.channel_id,
                                    st.time_slot,
                                    st.show_id,
                                    e.episode_id,
                                    st.runtime,
                                    st.replication_year,
                                    st.days_of_week
                                FROM
                                    schedule_template st
                                JOIN episodes e ON e.show_id = st.show_id
                                LEFT JOIN broadcast_log bl ON bl.episode_id = e.episode_id 
                                                           AND bl.channel_id = st.channel_id
                                WHERE 
                                    bl.episode_id IS NULL
                                    AND st.channel_id = %s
                                    AND st.replication_year = %s
                                    AND st.time_slot::text = %s
                                    AND %s = ANY(st.days_of_week)
                                ORDER BY e.show_season_number, e.episode_number
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

            # Execute the query and retrieve the next episode
            self.cur.execute(query,
                             (time_slot['channel_id'], time_slot['schedule_id'], time_slot['start_time'], day_of_week))
            episode_id = self.cur.fetchone()
            if episode_id is None:
                return None

            # Query to fetch detailed episode information
            query = sql.SQL("""SELECT e.*, bl.time_slot FROM episodes e 
                                    LEFT JOIN broadcast_log bl ON e.episode_id = bl.episode_id 
                                    WHERE e.episode_id = %s""")
            self.cur.execute(query, (episode_id['episode_id'],))
            formatted_record = [{**dict(record), 'type': 'episode'} for record in self.cur.fetchall()]
            return formatted_record[0]
        except Exception as e:
            # Log any errors encountered during the query
            logging.error(f"Could not get Next Episode: {e}")
        finally:
            self.db_connection.commit()  # Ensure the transaction is committed to the database

#!/usr/bin/env python3
import os
from pprint import pprint

import psycopg2
from dotenv import load_dotenv

from psycopg2.extras import DictCursor
from playlists.classes.Commercials import Commercials
from playlists.classes.Episodes import Episodes
from playlists.classes.Shows import Shows

load_dotenv()
# Path to the local storage, could be customized based on environment
LOCAL_PATH = os.getenv('LOCAL_PATH')
# Ratio used to sharpen the video in VLC
SHARPEN_RATIO = os.getenv('SHARPEN_RATIO')


# Class to manage the creation of playlists
class Playlists:
    def __init__(self, db: psycopg2.extensions.connection, hostname: str) -> None:
        """Initialize playlist class, connect to DB and set up file for writing."""
        self.db_connection = db
        self.cur = db.cursor(cursor_factory=DictCursor)  # Use DictCursor to return query results as dicts
        self.commercials = Commercials(self.db_connection)  # Commercials handler
        self.shows = Shows(self.db_connection, hostname)  # Shows handler
        self.playlist = open(f"{LOCAL_PATH}/sys/playlists/{hostname}_playlist.m3u", "w")
        self.playlist.write("#EXTM3U\n")  # Initialize M3U playlist with the EXTM3U header

    def close_playlist(self) -> None:
        """Close the playlist file."""
        self.playlist.close()

    def insert_signoff(self) -> None:
        """Insert sign-off content at the end of the playlist."""
        self.playlist.write(f"#EXTVLCOPT:sharpen-sigma={SHARPEN_RATIO}\n")
        self.playlist.write(f"{LOCAL_PATH}/signoff/high_flight_signoff.mp4\n")
        self.playlist.write("#EXTVLCOPT:image-duration=600\n")  # Set 10-minute duration for the image
        self.playlist.write(f"{LOCAL_PATH}/signoff/test_pattern.png\n")  # Add test pattern image

    def insert_commercial(self, slot: dict) -> None:
        """Insert a commercial into the playlist with specified start and end times."""
        sub_year = int(str(slot['commercial_airdate'])[-2:])  # Extract last two digits of the year
        decade = f"{str((sub_year - (sub_year % 10))).zfill(2)}s"  # Calculate decade
        com_file_path = f"{LOCAL_PATH}/{decade}/commercials/{slot['commercial_file']}"  # Path to the commercial file
        self.playlist.write(f"#EXTVLCOPT:start-time={slot['commercial_start']}\n")
        self.playlist.write(f"#EXTVLCOPT:stop-time={slot['commercial_end']}\n")
        self.playlist.write(f"#EXTVLCOPT:sharpen-sigma={SHARPEN_RATIO}\n")
        self.playlist.write(f"{com_file_path}\n")

    def get_playlist(self, final_episodes: list, holiday: str) -> None:
        """Generate the playlist by inserting episodes and associated commercials."""
        commercial_break_flag = None

        for episode in final_episodes:
            print(episode)
            movie_duration = None
            episode_true_duration = None
            commercial_break_counter = 0  # Initialize counter for commercial breaks

            #Handle episode-related logic
            if episode['type'] == 'episode':
                sub_year = int(str(episode['episode_airdate'].year)[-2:])
                decade = f"{str((sub_year - (sub_year % 10))).zfill(2)}s"
                file_path = f"{LOCAL_PATH}/{decade}/{str(sub_year).zfill(2)}/{episode['episode_file']}"
                commercial_breaks = self.commercials.get_commercial_breaks(episode['episode_id'])
                episode_true_duration = Episodes.get_true_duration(self.cur, episode['episode_id'])
                print(episode['show_id'], episode_true_duration)
                show_duration = self.shows.get_show_duration(episode['show_id'], episode_true_duration)
                target_duration = show_duration - episode_true_duration  # Remaining time to fill with commercials
                number_of_breaks = len(commercial_breaks) + 2  # Adding 2 breaks: before and after the show
                breaks = [[] for _ in range(number_of_breaks)]  # Create empty lists for each commercial break
                episode_breaks = list(filter(lambda b: b['media_id'] == episode['episode_id'], commercial_breaks))
                commercial_year = episode['episode_airdate'].year

            # Handle movie-related logic
            elif episode['type'] == 'movie':
                sub_year = int(str(episode['movie_release_date'])[-2:])
                decade = f"{str((sub_year - (sub_year % 10))).zfill(2)}s"
                file_path = f"{LOCAL_PATH}/{decade}/movies/{episode['movie_file']}"
                movie_duration = episode['end_point'] - episode['start_point']
                target_duration = episode['slot_duration'] - movie_duration
                number_of_breaks = 2  # Movies typically have fewer breaks
                breaks = [[] for _ in range(number_of_breaks)]
                episode_breaks = []
                commercial_year = episode['movie_release_date']

            elif episode['type'] == 'special':
                sub_year = int(str(episode['specials_airdate'])[-2:])
                decade = f"{str((sub_year - (sub_year % 10))).zfill(2)}s"
                file_path = f"{LOCAL_PATH}/{decade}/specials/{episode['specials_file']}"
                movie_duration = episode['end_point'] - episode['start_point']
                target_duration = episode['slot_duration'] - movie_duration
                number_of_breaks = 2  # Specials typically have fewer breaks
                breaks = [[] for _ in range(number_of_breaks)]
                episode_breaks = []
                commercial_year = episode['specials_airdate']
            else:
                raise Exception(f"Sorry, {episode['type']} is not known")

            if holiday:
                episode_breaks = []
                number_of_breaks = 2
                target_duration = movie_duration if movie_duration else episode_true_duration

            # Retrieve commercials based on target duration and year
            commercials = self.commercials.get_commercials(target_duration, commercial_year)
            commercial_ids = [(record['commercial_id'], record['duration']) for record in commercials]
            commercial_selector = self.commercials.randomized_search(commercial_ids, target_duration, max_attempts=100)

            # Distribute commercials across breaks evenly
            for i, commercial_id in enumerate(commercial_selector):
                breaks[i % number_of_breaks].append(commercial_id)

            # Insert first commercial break before the show starts
            break_commercials = [commercial for commercial in commercials if
                                 commercial['commercial_id'] in breaks[commercial_break_counter]]

            break_commercials = break_commercials[:1] if holiday else break_commercials

            for commercial in break_commercials:
                self.insert_commercial(commercial)
            commercial_break_counter += 1

            # If no episode breaks, insert the entire episode
            if len(episode_breaks) == 0:
                self.playlist.write(f"#EXTVLCOPT:start-time={episode['start_point']}\n")
                self.playlist.write(f"#EXTVLCOPT:stop-time={episode['end_point']}\n")
                self.playlist.write(f"#EXTVLCOPT:aspect-ratio={episode['aspect_ratio']}\n")
                self.playlist.write(f"#EXTVLCOPT:sharpen-sigma={SHARPEN_RATIO}\n")
                self.playlist.write(f"{file_path}\n")

            if not holiday:
                # Insert episode segments around breaks
                for i, episode_break in enumerate(episode_breaks):
                    if commercial_break_flag != episode['episode_id']:
                        # First break for the episode
                        commercial_break_flag = episode['episode_id']
                        self.playlist.write(f"#EXTVLCOPT:start-time={episode['start_point']}\n")
                        self.playlist.write(f"#EXTVLCOPT:stop-time={episode_break['break_point'] + 1}\n")
                    else:
                        # Breaks after the first
                        self.playlist.write(f"#EXTVLCOPT:start-time={episode_breaks[i - 1]['resume_point']}\n")
                        self.playlist.write(f"#EXTVLCOPT:stop-time={episode_break['break_point'] + 1}\n")

                    # Sharpen effect and episode path
                    self.playlist.write(f"#EXTVLCOPT:aspect-ratio={episode['aspect_ratio']}\n")
                    self.playlist.write(f"#EXTVLCOPT:sharpen-sigma={SHARPEN_RATIO}\n")
                    self.playlist.write(f"{file_path}\n")

                    # Insert commercials between episode segments
                    if number_of_breaks > 2:
                        break_commercials = [commercial for commercial in commercials if
                                             commercial['commercial_id'] in breaks[commercial_break_counter]]
                        for commercial in break_commercials:
                            self.insert_commercial(commercial)
                        commercial_break_counter += 1

                    # Final segment of the episode after the last break
                    if i == (len(episode_breaks) - 1):
                        self.playlist.write(f"#EXTVLCOPT:start-time={episode_break['resume_point']}\n")
                        self.playlist.write(f"#EXTVLCOPT:stop-time={episode['end_point']}\n")
                        self.playlist.write(f"#EXTVLCOPT:aspect-ratio={episode['aspect_ratio']}\n")
                        self.playlist.write(f"#EXTVLCOPT:sharpen-sigma={SHARPEN_RATIO}\n")
                        self.playlist.write(f"{file_path}\n")

            # Insert last commercial break
            break_commercials = [commercial for commercial in commercials if
                                 commercial['commercial_id'] in breaks[commercial_break_counter]]

            break_commercials = break_commercials[:1] if holiday else break_commercials

            for commercial in break_commercials:
                self.insert_commercial(commercial)

        # Insert sign-off and close playlist
        self.insert_signoff()
        self.close_playlist()

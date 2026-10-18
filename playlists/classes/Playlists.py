#!/usr/bin/env python3
import os
from pprint import pprint

import psycopg
from dotenv import load_dotenv

from psycopg.rows import dict_row
from playlists.classes.Commercials import Commercials, AUDIENCE_CHILDREN
from playlists.classes.Episodes import Episodes
from playlists.classes.Shows import Shows

load_dotenv()
# Path to the local storage, could be customized based on environment
LOCAL_PATH = os.getenv('LOCAL_PATH')
# Ratio used to sharpen the video in VLC
SHARPEN_RATIO = os.getenv('SHARPEN_RATIO')


# Class to manage the creation of playlists
class Playlists:
    def __init__(self, db: psycopg.Connection, hostname: str, is_dev_mode: bool = False) -> None:
        """Initialize playlist class, connect to DB and set up file for writing."""
        self.db_connection = db
        self.cur = db.cursor(row_factory=dict_row)  # Use dict_row to return query results as dicts
        self.commercials = Commercials(self.db_connection)  # Commercials handler
        self.shows = Shows(self.db_connection, hostname)  # Shows handler
        if not is_dev_mode:
            self.playlist = open(f"/Volumes/TTBS/time_traveler/sys/playlists/{hostname}_playlist.m3u", "w")
        else:
            self.playlist = open(f"./{hostname}_playlist.m3u", "w")
        self.playlist.write("#EXTM3U\n")  # Initialize M3U playlist with the EXTM3U header

    def close_playlist(self) -> None:
        """Close the playlist file."""
        self.playlist.close()

    def insert_signoff(self) -> None:
        """Insert sign-off content at the end of the playlist."""
        self.playlist.write(f"#EXTVLCOPT:start-time=0\n")
        self.playlist.write(f"#EXTVLCOPT:stop-time=133\n")
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

    def _audience_for(self, episode: dict, dow: int | None = None) -> str | None:
        """AUDIENCE_CHILDREN when ads must be kid-appropriate, else None (any commercial may run).
        The restriction is a MORNING rule: afternoon and primetime take any ad, even on cartoons.
        Before noon it applies to children's programming (a children's/cartoon/kids show or an
        interstitial like School House Rock) and to anything in a Saturday-morning slot, when the
        audience is kids whatever the show."""
        if episode.get('type') != 'episode':
            return None
        slot_time = episode.get('time_slot')
        if slot_time is not None and slot_time.hour >= 12:
            return None
        if episode.get('interstitial'):
            return AUDIENCE_CHILDREN

        show_id = episode['show_id']
        if not hasattr(self, '_childrens_show_cache'):
            self._childrens_show_cache = {}
        if show_id not in self._childrens_show_cache:
            self.cur.execute(
                """SELECT s.show_type, s.show_genre, e.mapped_genre
                   FROM shows s LEFT JOIN show_syndication_eligibility e USING (show_id)
                   WHERE s.show_id = %s""", (show_id,))
            row = self.cur.fetchone() or {}
            genre = (row.get('show_genre') or '').lower()
            self._childrens_show_cache[show_id] = (
                row.get('show_type') == 'children'
                or row.get('mapped_genre') in ('kids', 'interstitial')
                or 'cartoon' in genre or 'child' in genre
            )
        if self._childrens_show_cache[show_id]:
            return AUDIENCE_CHILDREN

        if dow == 5 and slot_time is not None and 7 <= slot_time.hour < 12:
            return AUDIENCE_CHILDREN
        return None

    @staticmethod
    def _interstitial_seconds_after(final_episodes: list, index: int) -> float:
        """Seconds of interstitial (School House Rock) that immediately follows final_episodes[index].
        It plays in that program's closing break, so its time comes OUT of the break's commercial
        budget instead of extending the schedule."""
        if index + 1 < len(final_episodes):
            nxt = final_episodes[index + 1]
            if nxt.get('type') == 'episode' and nxt.get('interstitial'):
                return float(nxt['end_point'] - nxt['start_point'])
        return 0.0

    @staticmethod
    def _episode_path(episode: dict) -> str:
        sub_year = int(str(episode['episode_airdate'].year)[-2:])
        decade = f"{str((sub_year - (sub_year % 10))).zfill(2)}s"
        return f"{LOCAL_PATH}/{decade}/{str(sub_year).zfill(2)}/{episode['episode_file']}"

    def _write_episode_content(self, episode: dict) -> None:
        """Write an episode's program content with no commercial pods, cutting out the original
        ad breaks (commercial_breaks) so only real content airs -- the same cut points the normal
        path uses, which is what keeps a block's airtime equal to its content plus era-matched ads."""
        file_path = self._episode_path(episode)
        cursor = episode['start_point']
        segments = []
        for brk in self.commercials.get_commercial_breaks(episode['episode_id']):
            if brk['break_point'] > cursor:
                segments.append((cursor, brk['break_point']))
            cursor = max(cursor, brk['resume_point'])
        segments.append((cursor, episode['end_point']))
        for start, stop in segments:
            self.playlist.write(f"#EXTVLCOPT:start-time={start}\n")
            self.playlist.write(f"#EXTVLCOPT:stop-time={stop}\n")
            self.playlist.write(f"#EXTVLCOPT:aspect-ratio={episode['aspect_ratio']}\n")
            self.playlist.write(f"#EXTVLCOPT:sharpen-sigma={SHARPEN_RATIO}\n")
            self.playlist.write(f"{file_path}\n")

    def _build_pack_pods(self, members: list, audience: str | None = None, reserved_seconds: float = 0.0) -> list:
        """Commercial pods for a block of packed shorts: one pod before each short plus one after
        the last (len(members) + 1). The block's commercial time is the slot length minus the
        shorts' combined content, chosen once and spread evenly across the pods -- never per short,
        which would give every 5-minute short a full half hour of ads."""
        content = sum(float(Episodes.get_true_duration(self.cur, m['episode_id'])) for m in members)
        target = max(0.0, float(members[0]['pack_slot_seconds']) - content - reserved_seconds)
        commercials = self.commercials.get_commercials(target, members[0]['replication_year'], audience=audience)
        by_id = {c['commercial_id']: c for c in commercials}
        chosen = self.commercials.randomized_search(
            [(c['commercial_id'], float(c['duration'])) for c in commercials], target, max_attempts=100)
        pods = [[] for _ in range(len(members) + 1)]
        for i, commercial_id in enumerate(chosen):
            pods[i % len(pods)].append(by_id[commercial_id])
        return pods

    def get_playlist(self, final_episodes: list, holiday: str | None, dow: int | None = None) -> None:
        """Generate the playlist by inserting episodes and associated commercials."""
        commercial_break_flag = None
        pack_pods = {}  # pack_id -> commercial pods, built once per block of packed shorts

        for index, episode in enumerate(final_episodes):
            # Interstitials (e.g. School House Rock) air in the break between programs: the clip
            # alone, no commercial pods of their own.
            if episode['type'] == 'episode' and episode.get('interstitial'):
                self._write_episode_content(episode)
                continue

            # Packed shorts: one commercial budget for the whole block, a pod before each short and
            # one after the last. (Holiday playlists keep the normal per-episode handling.)
            if episode['type'] == 'episode' and episode.get('pack_size', 1) > 1 and not holiday:
                pack_id = episode['pack_id']
                if pack_id not in pack_pods:
                    members = [e for e in final_episodes if e.get('pack_id') == pack_id and e['type'] == 'episode']
                    last_index = max(i for i, e in enumerate(final_episodes) if e.get('pack_id') == pack_id)
                    pack_pods[pack_id] = self._build_pack_pods(
                        members, self._audience_for(episode, dow),
                        self._interstitial_seconds_after(final_episodes, last_index))
                pods = pack_pods[pack_id]
                for commercial in pods[episode['pack_index']]:
                    self.insert_commercial(commercial)
                self._write_episode_content(episode)
                if episode['pack_index'] == episode['pack_size'] - 1:
                    for commercial in pods[-1]:
                        self.insert_commercial(commercial)
                continue

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
                show_duration = self.shows.get_show_duration(episode['show_id'], episode_true_duration)
                target_duration = show_duration - episode_true_duration  # Remaining time to fill with commercials
                reserved = self._interstitial_seconds_after(final_episodes, index)
                if reserved:  # an interstitial airs in this program's closing break
                    target_duration = max(target_duration - type(target_duration)(reserved), type(target_duration)(0))
                number_of_breaks = len(commercial_breaks) + 2  # Adding 2 breaks: before and after the show
                breaks = [[] for _ in range(number_of_breaks)]  # Create empty lists for each commercial break
                episode_breaks = list(filter(lambda b: b['media_id'] == episode['episode_id'], commercial_breaks))
                commercial_year = episode["replication_year"]

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
                commercial_year = episode["replication_year"]

            elif episode['type'] == 'special':
                sub_year = int(str(episode['specials_airdate'])[-2:])
                decade = f"{str((sub_year - (sub_year % 10))).zfill(2)}s"
                file_path = f"{LOCAL_PATH}/{decade}/specials/{episode['specials_file']}"
                movie_duration = episode['end_point'] - episode['start_point']
                target_duration = episode['slot_duration'] - movie_duration
                number_of_breaks = 2  # Specials typically have fewer breaks
                breaks = [[] for _ in range(number_of_breaks)]
                episode_breaks = []
                commercial_year = episode["replication_year"]
            else:
                raise Exception(f"Sorry, {episode['type']} is not known")

            if holiday:
                episode_breaks = []
                number_of_breaks = 2
                target_duration = movie_duration if movie_duration else episode_true_duration

            # Retrieve commercials based on target duration and year
            commercials = self.commercials.get_commercials(
                target_duration, commercial_year, audience=self._audience_for(episode, dow))
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
                        self.playlist.write(f"#EXTVLCOPT:stop-time={episode_break['break_point']}\n")
                    else:
                        # Breaks after the first
                        self.playlist.write(f"#EXTVLCOPT:start-time={episode_breaks[i - 1]['resume_point']}\n")
                        self.playlist.write(f"#EXTVLCOPT:stop-time={episode_break['break_point']}\n")

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

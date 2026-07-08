import contextlib
import io
import json
import random
import shutil
import string
from time import sleep
import numpy as np
import matplotlib
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import seaborn as sns
import cv2
import math
import os
import re
import hashlib
import subprocess
from pathlib import Path
from urllib.parse import urlparse

from matplotlib.widgets import Button
from sklearn.metrics import confusion_matrix
from typing import Union, List, Optional
from PIL import Image

load_dotenv()

class IsBlackWhite:

    @staticmethod
    def insert_bw(episode_id: int, is_bw: bool):

        db_config = {
            'dbname': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': os.getenv("DB_HOST"),
            'port': os.getenv("DB_PORT"),
        }

        update_query = f"""UPDATE episodes SET is_bw = %s WHERE episode_id = %s;"""


        try:
            conn = psycopg2.connect(**db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(update_query, (episode_id, is_bw))
            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            print(f"Database query failed: {e}")
            return None

    @staticmethod
    def is_bw_frame(frame, tolerance=5):
        return np.all(np.abs(frame[:, :, 0] - frame[:, :, 1]) < tolerance) and \
            np.all(np.abs(frame[:, :, 1] - frame[:, :, 2]) < tolerance)

    @staticmethod
    def is_video_black_and_white_opencv(video_path, max_samples=20):
        cap = cv2.VideoCapture(video_path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        step = max(total_frames // max_samples, 1)
        count_bw = 0
        for i in range(0, total_frames, step):
            cap.set(cv2.CAP_PROP_POS_FRAMES, i)
            ret, frame = cap.read()
            if not ret:
                continue
            if IsBlackWhite.is_bw_frame(frame):
                count_bw += 1
        cap.release()
        return count_bw >= max_samples * 0.95  # 95% of sampled frames are grayscale


class CommercialBreaks:
    @staticmethod
    def get_episode_name(db_config, episode_ids: list):
        query = f"""SELECT episode_id, episode_file, episode_airdate FROM episodes WHERE episode_id = ANY(%s);"""
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, (episode_ids,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows


    @staticmethod
    def get_show_name(db_config, show_id):
        query = f"""SELECT show_name FROM shows WHERE show_id = {show_id};"""
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query)
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row['show_name']

    @staticmethod
    def analyze_break_outliers(db_config, show_id, z_thresh: float = 2.0):
        query = f"""
            SELECT
                s.show_id,
                e.episode_id,
                cb.break_point,
                cb.resume_point
            FROM commercial_breaks cb
            JOIN episodes e ON cb.media_id = e.episode_id
            JOIN shows s ON e.show_id = s.show_id
            WHERE s.show_id = {show_id}
            ORDER BY s.show_id, e.episode_id, cb.break_point;
        """
        conn = psycopg2.connect(**db_config)
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        # Derived fields
        df['break_duration'] = df['resume_point'] - df['break_point']
        df['break_midpoint'] = (df['break_point'] + df['resume_point']) / 2
        df['break_index'] = df.groupby('episode_id').cumcount() + 1

        # --- 1. Outliers by number of breaks per episode ---
        breaks_per_ep = (
            df.groupby(['show_id', 'episode_id'])['break_index']
            .max().reset_index(name='num_breaks')
        )

        stats = breaks_per_ep.groupby('show_id')['num_breaks'].agg(['mean', 'std']).reset_index()
        merged = pd.merge(breaks_per_ep, stats, on='show_id')
        merged['num_breaks_z'] = (merged['num_breaks'] - merged['mean']) / merged['std']
        merged['break_count_outlier'] = merged['num_breaks_z'].abs() > z_thresh

        # --- 2. Outliers by unusual break location (per index) ---
        stats_idx = (
            df.groupby(['show_id', 'break_index'])['break_midpoint']
            .agg(avg_midpoint='mean', std_midpoint='std')
            .reset_index()
        )

        df = df.merge(stats_idx, on=['show_id', 'break_index'], how='left', validate='many_to_one')

        df['midpoint_z'] = (df['break_midpoint'] - df['avg_midpoint']) / df['std_midpoint']
        df.loc[df['std_midpoint'] == 0, 'midpoint_z'] = np.nan
        df['location_outlier'] = df['midpoint_z'].abs() > z_thresh

        # --- 3. Average break midpoints ---
        avg_midpoints = (
            df.groupby(['show_id', 'break_index'])['break_midpoint']
            .mean().reset_index(name='avg_break_midpoint')
        )

        # Collect results
        episode_outliers = merged[merged['break_count_outlier']]
        break_outliers = df[df['location_outlier']]

        episode_outlier_breaks = df[df['episode_id'].isin(episode_outliers['episode_id'])][
            ['show_id', 'episode_id', 'break_index', 'break_midpoint']
        ].copy()

        return episode_outliers, break_outliers, avg_midpoints, episode_outlier_breaks

    @staticmethod
    def create_report(show_id: int, db_config: dict):
        episode_outliers, break_outliers, avg_breaks, episode_outlier_breaks = CommercialBreaks.analyze_break_outliers(db_config,show_id)
        show_name = CommercialBreaks.get_show_name(db_config, show_id)
        print(show_name)

        episodes = CommercialBreaks.get_episode_name(db_config, episode_outliers['episode_id'].tolist())
        for row in episodes:
            print(f"{row['episode_id']}     {row['episode_file']}     {row['episode_airdate']}")

        print("\nEpisodes with abnormal number of breaks:")
        print(episode_outliers[['show_id', 'episode_id', 'num_breaks', 'mean', 'num_breaks_z']])

        episode_outlier_breaks['break_midpoint_fmt'] = episode_outlier_breaks['break_midpoint'].apply(CommercialBreaks.format_time)

        print("\nBreak midpoints for episodes with abnormal number of breaks:")
        print(episode_outlier_breaks)

        print("\nAverage break midpoints across episodes:")
        avg_breaks['avg_break_midpoint_fmt'] = avg_breaks['avg_break_midpoint'].apply(CommercialBreaks.format_time)
        print(avg_breaks[['show_id', 'break_index', 'avg_break_midpoint', 'avg_break_midpoint_fmt']])

        print("\nBreaks at unusual locations:")
        print(break_outliers[['show_id', 'episode_id', 'break_index',
                              'break_midpoint', 'avg_midpoint', 'std_midpoint', 'midpoint_z']])

    @staticmethod
    def insert_commercial_break(episode_id: int, breaks=None):
        if breaks is None:
            breaks = []

        db_config = {
            'dbname': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': os.getenv("DB_HOST"),
            'port': os.getenv("DB_PORT"),
        }

        insert_query = f"""INSERT INTO commercial_breaks (media_id, break_point, resume_point) VALUES (%s, %s, %s);"""
        data = [(episode_id, round(start,2), round(end, 2)) for start, end in breaks]

        try:
            conn = psycopg2.connect(**db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.executemany(insert_query, data)
            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            print(f"Database query failed: {db_config}")
            return None

    @staticmethod
    def run_ffmpeg_blackdetect(video_path, duration: float = 0.8, threshold: float = 0.1):
        """Extract black screen segments using ffmpeg."""
        cmd = [
            "ffmpeg", "-i", video_path,
            "-vf", f"blackdetect=d={duration}:pix_th={threshold}",
            "-an", "-f", "null", "-"
        ]
        result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
        black_segments = []

        for line in result.stderr.splitlines():
            match = re.search(r"black_start:(\d+\.?\d*)\s+black_end:(\d+\.?\d*)", line)
            if match:
                black_segments.append((float(match.group(1)), float(match.group(2))))
        return black_segments

    @staticmethod
    def run_ffmpeg_silencedetect(video_path, db: float = 50, duration: float = 0.2):
        """Extract silence segments using ffmpeg."""
        cmd = [
            "ffmpeg", "-i", video_path,
            "-af", f"silencedetect=n=-{db}dB:d={duration}",  # silence = below -50dB for 1 second
            "-f", "null", "-"
        ]
        result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
        silence_segments = []
        start = None

        for line in result.stderr.splitlines():
            if "silence_start" in line:
                start = float(line.split("silence_start: ")[1])
            elif "silence_end" in line and start is not None:
                end = float(line.split("silence_end: ")[1].split()[0])
                silence_segments.append((start, end))
                start = None
        return silence_segments

    @staticmethod
    def merge_segments(black, silence, tolerance=1.0):
        """Find overlapping or close black+silence segments."""
        commercials = []
        for b_start, b_end in black:
            for s_start, s_end in silence:
                latest_start = max(b_start, s_start)
                earliest_end = min(b_end, s_end)
                overlap = earliest_end - latest_start

                if overlap >= 0 or abs(b_end - s_start) <= tolerance or abs(s_end - b_start) <= tolerance:
                    commercials.append((min(b_start, s_start), max(b_end, s_end)))
        return CommercialBreaks.merge_close_segments(commercials)

    @staticmethod
    def merge_close_segments(segments, max_gap=1.0):
        """Merge segments that are close together."""
        if not segments:
            return []

        segments.sort()
        merged = [segments[0]]

        for current in segments[1:]:
            last = merged[-1]
            if current[0] - last[1] <= max_gap:
                merged[-1] = (last[0], max(last[1], current[1]))
            else:
                merged.append(current)
        return merged

    @staticmethod
    def filter_edges(segments, start_point=0.0, end_point=None, epsilon=0.2):
        """
        Remove first segment if it starts near start_point and
        last if it ends near end_point.

        Args:
            segments (list of (start, end)): list of segments
            start_point (float): lower bound to treat as 'start edge'
            end_point (float): upper bound to treat as 'end edge'
            epsilon (float): tolerance when comparing
        """
        if not segments:
            return []

            # Drop all segments starting before start_point + epsilon
        filtered = [seg for seg in segments if seg[0] > start_point + epsilon]

        # Drop all segments ending after end_point - epsilon, if end_point is given
        if end_point is not None:
            filtered = [seg for seg in filtered if seg[1] < end_point - epsilon]

        return filtered

    @staticmethod
    def format_time(seconds: float) -> str:
        return f"{int(seconds // 3600):02}:{int((seconds % 3600) // 60):02}:{seconds % 60:06.3f}"


class VideoReProcess:

    @staticmethod
    def get_video_length(filename: str) -> Optional[float]:
        """Return video duration in seconds, or None if not available."""
        try:
            stderr_capture = io.StringIO()
            with contextlib.redirect_stderr(stderr_capture):
                cap = cv2.VideoCapture(filename)
                fps = cap.get(cv2.CAP_PROP_FPS)

                # Get any warnings from OpenCV
                warning_msg = stderr_capture.getvalue()
                if warning_msg:
                    print(f"OpenCV Warning: {warning_msg.strip()}")
                    # Handle warning here, e.g., retry with fallback settings

            if fps <= 0:
                raise ValueError("Invalid FPS value")
            duration = cap.get(cv2.CAP_PROP_FRAME_COUNT) / fps
            cap.release()
            return round(duration, 2)
        except Exception as e:
            print(f"Error probing video {filename}: {e}")
            return None

    @staticmethod
    def repair_video(file_path: str) -> bool | str:
        """
        Repair a potentially corrupted MP4 by re-encoding video and audio.

        Args:
            file_path (str): Path to the source video file.
            output_file (str): Path to save the repaired video.

        Returns:
            bool: True if successful, False otherwise.
        """
        input_path = Path(file_path)
        output_path = Path(f"/tmp/REPAIRED_{VideoReProcess._random_filename()}")

        if not input_path.exists():
            print(f"Error: File not found - {file_path}")
            return False

        cmd = [
            "ffmpeg",
            "-y",  # overwrite output if it exists
            "-err_detect", "ignore_err",  # ignore decode errors
            "-i", str(input_path),
            "-c:v", "libx264",  # re-encode video to H.264
            "-c:a", "aac",  # re-encode audio to AAC
            "-strict", "-2",  # allow experimental AAC encoder (legacy)
            str(output_path)
        ]

        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            print(f"Repaired file saved to: {output_path}")
            return str(output_path)
        except subprocess.CalledProcessError as e:
            print("FFmpeg error:\n", e.stderr)
            return False


    @staticmethod
    def get_metadata(file_path: str) -> dict:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            file_path
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return json.loads(result.stdout)

    @staticmethod
    def get_random_filename():
        return VideoReProcess._random_filename()

    @staticmethod
    def _random_filename(length: int = 10) -> str:
        """Generate a random JSON filename."""
        chars = string.ascii_letters + string.digits
        name = ''.join(random.choices(chars, k=length))
        return f"TEMP_{name}.mp4"

    @staticmethod
    def update_video_metadata(input_file: str, metadata: dict[str, str]) -> bool:
        """
        Clears existing metadata in a video file and replaces it with custom metadata.

        Args:
            input_file (str): Path to the source video file.
            output_file (str): Path to the output video file.
            metadata (dict): Dictionary of metadata key/value pairs.
                             Example: {"title": "My Video", "artist": "Me", "comment": "Test"}

        Returns:
            bool: True if successful, False otherwise.
        """
        input_path = Path(input_file)
        temp_file_name = VideoReProcess._random_filename()
        temp_output_file = f"/tmp/meta_{temp_file_name}"

        if not input_path.exists():
            print(f"Error: File not found - {input_file}")
            return False

        # Build ffmpeg command
        cmd = [
            "ffmpeg", "-y",
            "-i", str(input_path),
            "-map", "0",
            "-map_metadata", "-1",
            "-c:v", "copy",
            "-c:a", "copy"
        ]

        # Add metadata key/values
        for k, v in metadata.items():
            cmd.extend(["-metadata", f"{k}={v}"])

        cmd.append(str(temp_output_file))

        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            shutil.copy2(temp_output_file, f"{input_path.parent}/{temp_file_name}")
            if VideoReProcess.check_video(f"{input_path.parent}/{temp_file_name}"):
                os.rename(input_path, f"{input_path.parent}/OLD_{input_path.name}")
                os.rename(f"{input_path.parent}/{temp_file_name}", input_file)
                os.remove(temp_output_file)
                return True
            else:
                print(f"[ERROR] Processed file for {str(input_path)} is corrupt and will not be moved.")
                os.remove(temp_output_file)
                os.remove(f"{input_path.parent}/{temp_file_name}")
                return False

        except subprocess.CalledProcessError as e:
            print("FFmpeg error:", e.stderr.decode())
            # Clean up temp file if it was created
            if os.path.exists(temp_output_file):
                try:
                    os.remove(temp_output_file)
                    print(f"Deleted temp file: {temp_output_file}")
                except OSError as cleanup_err:
                    print(f"Failed to delete temp file: {cleanup_err}")

            return False

    @staticmethod
    def check_video(file_path: str) -> tuple[bool, str | None]:
        """
        Returns (True, None) if the video decodes without errors.
        Returns (False, error_message) if corrupt or ffmpeg fails.
        """
        try:
            result = subprocess.run(
                ["ffmpeg", "-v", "error", "-i", file_path, "-f", "null", "-"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            if result.stderr.strip():
                # Return False + the error string
                return False, result.stderr.strip()
            return True, None
        except Exception as e:
            return False, str(e)

    @staticmethod
    def reprocess(input_file: str, metadata: dict[str, str]) -> Optional[bool]:
        """
        Transcode video with automatic bar cropping based on detected crop values.
        Produces a temporary `.build.mp4`, then renames it back to `input_file`.
        """
        try:
            a = urlparse(input_file)
            file = Path(os.path.basename(a.path))
            filename = str(file.with_suffix(''))  # base name without extension
            parent_path = Path(input_file).parent

            # Step 1: detect crop values
            cropdetect_cmd = ['ffmpeg', '-i', input_file, '-vf', 'cropdetect', '-f', 'null', '-']
            result = subprocess.run(cropdetect_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            counter = {}
            for line in result.stderr.splitlines():
                match = re.search(r'crop=(\d+:\d+:\d+:\d+)', line)
                if match:
                    crop_str = match.group(1)
                    hash_str = hashlib.md5(crop_str.encode()).hexdigest()
                    counter[hash_str] = counter.get(hash_str, []) + [crop_str]

            if not counter:
                raise RuntimeError("No crop values detected.")

            largest_list_key = max(counter, key=lambda k: len(counter[k]))
            crop_values = counter[largest_list_key].pop()

            # Step 2: define temporary output
            # temp_output_file = f"{parent_path}/{filename}.build.mp4"
            temp_file_name = VideoReProcess._random_filename()
            temp_output_file = f"/tmp/reprocess_{temp_file_name}"

            # Step 3: run ffmpeg crop + transcode
            crop_cmd = [
                'ffmpeg', '-y', '-loglevel', 'quiet', '-i', input_file,
                '-r', '30',
                "-map_metadata", "-1",
                '-vf', f'crop={crop_values},scale=min(512,iw):-2'
                '-af', 'loudnorm=I=-26:TP=-2:LRA=7',
                '-b:v', '800k',
                '-c:v', 'h264_videotoolbox',
                '-c:a', 'aac',
                '-b:a', '128k',
            ]

            # Add metadata key/values
            for k, v in metadata.items():
                crop_cmd.extend(["-metadata", f"{k}={v}"])

            crop_cmd.append(str(temp_output_file))

            subprocess.run(crop_cmd, check=True)

            # Copy the file to the destination

            shutil.copy2(temp_output_file, f"{parent_path}/{temp_file_name}")
            if VideoReProcess.check_video(f"{parent_path}/{temp_file_name}"):
                os.rename(input_file, f"{parent_path}/OLD_{file}")
                os.rename(f"{parent_path}/{temp_file_name}", input_file)
                os.remove(temp_output_file)
                return True
            else:
                print(f"[ERROR] Processed file for {input_file} is corrupt and will not be moved.")
                os.remove(temp_output_file)
                os.remove(f"{parent_path}/{temp_file_name}")
                return False

        except Exception as e:
            print(f"[ERROR] process_remove_bars failed: {e}")
            return None



"""
Module: video_contact_sheet
---------------------------
This module defines the VideoContactSheet class for extracting frames from
a video file and compiling them into a visual contact sheet. It supports
extracting specific frames by timestamp or evenly spaced frames over an interval.

    vcs = VideoContactSheet(video_path, cols=5)

    # Extract specific frame
    vcs.get_frame_at(5.5)

    # Extract frames at interval
    # vcs.extract_frames_interval(start_sec=0, end_sec=22.7, interval_sec=0.3)

    # Display contact sheet
    vcs.show_contact_sheet()
"""


class VideoContactSheet:
    """
    A class for extracting frames from a video and compiling them into
    a contact sheet image.

    Attributes:
        video_path (str): Path to the video file.
        cols (int): Number of columns in the contact sheet.
        frames (list[Image.Image]): List of extracted PIL image frames.
    """

    def __init__(self, video_path: str, episode_id: int, metadata=None, matlib_show: bool = True, cols: int = 6) -> None:
        if metadata is None:
            metadata = {}
        from celery.classes import VideoAnnotationGenerator
        """
        Initialize the VideoContactSheet.

        Args:
            video_path: Path to the input video file.
            cols: Number of columns in the contact sheet.
        """
        self.video_path = video_path
        self.new_metadata: dict = metadata
        self.episode_id = episode_id
        self.cols = cols
        self.frames: list[Image.Image | None] = []
        self.metadata: list[tuple[float, tuple[float, float]] | None] = []
        self.clip_metadata: list[tuple[float, tuple[float, float]]] = []
        self.annotations: list = []
        self.generator = VideoAnnotationGenerator()

        if not matlib_show:
            matplotlib.use("Agg")

    @staticmethod
    def frame_to_image(frame) -> Image.Image:
        """
        Convert an OpenCV BGR frame to a PIL Image in RGB format.

        Args:
            frame: OpenCV frame (numpy array).

        Returns:
            PIL Image object.
        """
        return Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))

    def get_frame_at(self, timestamps: Union[float, int, List[Union[float, int]]]) -> None:
        """
        Grab one or more frames at specific timestamps (in seconds)
        and append them to the contact sheet frames list.

        Args:
            timestamps: A single float/int or a list of floats/ints (seconds).
        """
        try:
            # Normalize to list of timestamps
            if isinstance(timestamps, (float, int)):
                timestamps = [timestamps]

            cap = cv2.VideoCapture(self.video_path)
            if not cap.isOpened():
                raise IOError(f"Cannot open video: {self.video_path}")

            for ts in timestamps:
                cap.set(cv2.CAP_PROP_POS_MSEC, float(ts) * 1000)
                ret, frame = cap.read()
                if ret:
                    self.frames.append(self.frame_to_image(frame))
                else:
                    print(f"[WARNING] Could not read frame at {ts} seconds.")

            cap.release()
        except Exception as e:
            print(f"[ERROR] Failed to get frame(s) at {timestamps}: {e}")

    def extract_frames_intervals(self, segments: list[tuple[float, float]], interval_sec: float,
                                 on_click: bool = False) -> None:
        """
        Extract frames at a fixed interval for multiple (start, end) segments.

        Args:
            segments: List of (start_sec, end_sec) tuples.
            interval_sec: Interval between frames in seconds.

        Parameters
        ----------
        interval_sec
        segments
        on_click
        """
        try:
            cap = cv2.VideoCapture(self.video_path)
            if not cap.isOpened():
                raise IOError(f"Cannot open video: {self.video_path}")

            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            duration = total_frames / fps if fps else 0

            self.frames.clear()
            self.clip_metadata.clear()
            if not on_click:
                self.metadata.clear()

            for i, (start_sec, end_sec) in enumerate(segments):
                # Adjust invalid times
                if end_sec > duration or end_sec <= 0:
                    end_sec = duration
                if start_sec < 0:
                    start_sec = 0

                t = start_sec
                while t <= end_sec:
                    cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
                    ret, frame = cap.read()
                    if not ret:
                        break

                    timestamp_text = f"{t:0>8.3f}s"
                    if not on_click:
                        self.metadata.append((t, (start_sec, end_sec)))
                    else:
                        self.clip_metadata.append((t, (start_sec, end_sec)))

                    cv2.putText(
                        frame,
                        timestamp_text,
                        (10, 30),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        1,
                        (0, 255, 0),
                        2,
                        cv2.LINE_AA
                    )

                    self.frames.append(self.frame_to_image(frame))
                    t += interval_sec
                self.frames.append(None)
                self.metadata.append(None)

                # Insert a marker for a line break between segments
               # if i < len(segments) - 1:
                #    self.frames.append(None)
                #    self.metadata.append(None)

            cap.release()

        except Exception as e:
            print(f"[ERROR] Failed to extract frames from segments {segments} every {interval_sec}s: {e}")

    def make_contact_sheet(self, return_grid: bool = False, cols: int = 6) -> tuple[Image.Image, tuple[int, int]] | Image.Image | None:
        """
        Create a contact sheet from the collected frames, inserting a line break between segments.

        Args:
            return_grid: If True, also return (rows, cols) grid shape.

        Returns:
            If return_grid is False:
                A PIL Image representing the contact sheet, or None if no frames exist.
            If return_grid is True:
                (sheet, (rows, cols)), or None if no frames exist.

        Parameters
        ----------
        return_grid
        cols
        """
        try:
            if not self.frames:
                raise ValueError("No frames extracted to make contact sheet.")

            w, h = self.frames[0].size

            # Count actual frames to compute rows
            num_actual_frames = len([f for f in self.frames if f is not None])
            # Estimate rows; we'll adjust y dynamically for line breaks
            rows = math.ceil(num_actual_frames / cols) + len([f for f in self.frames if f is None])
            sheet = Image.new("RGB", (cols * w, rows * h), color="white")
            x_offset = 0
            y_offset = 0
            col_count = 0

            for frame in self.frames:
                if frame is None:
                    # Line break between segments
                    x_offset = 0
                    y_offset += h
                    col_count = 0
                    continue

                sheet.paste(frame, (x_offset, y_offset))

                col_count += 1
                x_offset += w

                if col_count >= cols:
                    # Move to next row
                    x_offset = 0
                    y_offset += h
                    col_count = 0

            if return_grid:
                return sheet, (rows, cols)
            else:
                return sheet

        except Exception as e:
            print(f"[ERROR] Failed to create contact sheet: {e}")
            return None

    def save_contact_sheet(self, output_path: str) -> None:
        """
        Generate and save the contact sheet to disk without displaying it.
        """
        try:
            sheet, grid_shape = self.make_contact_sheet(return_grid=True)
            if sheet is None:
                return

            # Create a figure for saving (no interactive buttons)
            fig, ax = plt.subplots(figsize=(16, 12))
            ax.imshow(np.array(sheet))
            ax.axis('off')
            ax.set_title(self.video_path, fontsize=12, pad=20)

            # Save the contact sheet
            fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
            plt.close(fig)

        except Exception as e:
            print(f"[ERROR] Failed to save contact sheet: {e}")

    def show_contact_sheet(self, image_click: bool = False) -> None:
        """
        Display the generated contact sheet using matplotlib.
        """
        try:
            cols = 16 if image_click else 6
            sheet, grid_shape = self.make_contact_sheet(return_grid=True, cols=cols)
            if sheet is None:
                return

            # Create a bigger figure (width, height) in inches
            fig, ax = plt.subplots(figsize=(15, 15))
            ax.imshow(np.array(sheet))
            ax.axis('off')
            plt.title(self.video_path, fontsize=22, pad=20)

            # -------------------------
            # Button Handlers
            # -------------------------

            def on_manual_click(event):
                self.generator.manual_update_start_end(self.annotations, self.video_path)
                self.generator.send_video_to_queue(self.episode_id)
                self.annotations.clear()
                plt.close(fig)

            def on_content_click(event):
                self.generator.get_training_annotations((self.video_path, []))
                self.generator.update_in_dataset(self.video_path)
                plt.close(fig)

            def on_skip_click(event):
                self.generator.update_in_dataset(self.video_path)
                plt.close(fig)

            def on_clear_click(event):
                self.annotations.clear()

            def on_button_click(event):
                if self.annotations:
                    self.generator.get_training_annotations((self.video_path, self.annotations))
                    self.generator.update_in_dataset(self.video_path)
                    self.annotations.clear()
                plt.close(fig)

            # -------------------------
            # Buttons
            # -------------------------
            if not image_click:
                button_ax = plt.axes((0.8, 0.25, 0.1, 0.05))
                btn = Button(button_ax, "Process")
                btn.on_clicked(on_button_click)

                button_content = plt.axes((0.65, 0.25, 0.1, 0.05))
                btn_o = Button(button_content, "Content Only")
                btn_o.on_clicked(on_content_click)

                button_skip = plt.axes((0.35, 0.25, 0.1, 0.05))
                btn_s = Button(button_skip, "Skip")
                btn_s.on_clicked(on_skip_click)

                button_manual = plt.axes((0.50, 0.25, 0.1, 0.05))
                btn_m = Button(button_manual, "Manual Update")
                btn_m.on_clicked(on_manual_click)

                button_clear = plt.axes((0.2, 0.25, 0.1, 0.05))
                btn_c = Button(button_clear, "Clear")
                btn_c.on_clicked(on_clear_click)

            # -------------------------
            # Click Handlers
            # -------------------------
            nrows, ncols = grid_shape
            w, h = sheet.size
            cell_w = w / ncols
            cell_h = h / nrows

            def on_click(event):
                if event.inaxes != ax:
                    return
                col = int(event.xdata // cell_w)
                row = int(event.ydata // cell_h)
                idx = row * ncols + col
                if 0 <= idx < len(self.metadata):
                    _, start_end = self.metadata[idx]
                    self.extract_frames_intervals([start_end], 0.2, on_click=True)
                    self.show_contact_sheet(image_click=True)

            def second_click(event):
                if event.inaxes != ax:
                    return
                col = int(event.xdata // cell_w)
                row = int(event.ydata // cell_h)
                idx = row * ncols + col
                if 0 <= idx < len(self.clip_metadata):
                    meta = self.clip_metadata[idx]
                    self.annotations.append(meta)
                    sleep(0.5)
                    plt.close(fig)

            # -------------------------
            # Show logic
            # -------------------------
            if image_click:
                # create the button axes on this figure explicitly
                button_close_ax = fig.add_axes([0.5, 0.02, 0.1, 0.05])
                btn_cl = Button(button_close_ax, "Close")

                def on_button_close(event):
                    print("Close button clicked!")  # debug
                    plt.close(fig)

                btn_cl.on_clicked(on_button_close)

                fig.canvas.mpl_connect("button_press_event", second_click)
                plt.show(block=False)
            else:
                ...
                fig.canvas.mpl_connect("button_press_event", on_click)
                plt.show()

        except Exception as e:
            print(f"[ERROR] Failed to display contact sheet: {e}")


class ConfusionMatrix:
    """
    Class to compute and plot a confusion matrix using true and predicted labels.
    """

    def __init__(self, target_classes: Optional[List[str]] = None):
        """
        Initialize the plotter.

        Args:
            target_classes: List of class names or labels for axis ticks.
        """
        self.target_classes = target_classes or []
        self.true_labels = []
        self.predicted_labels = []

    def set_labels(self, true_labels: List[int], predicted_labels: List[int]) -> None:
        """
        Set the true and predicted labels.

        Args:
            true_labels: List of ground truth labels.
            predicted_labels: List of predicted labels by the model.
        """
        self.true_labels = true_labels
        self.predicted_labels = predicted_labels

    def plot(self, epoch: int = 0) -> None:
        """
        Plot the confusion matrix heatmap.

        Args:
            epoch: Epoch number for the plot title.
        """
        if not self.true_labels or not self.predicted_labels:
            print("True labels or predicted labels are empty. Cannot plot confusion matrix.")
            return

        cm = confusion_matrix(self.true_labels, self.predicted_labels)
        plt.figure(figsize=(6, 5))
        sns.heatmap(
            cm,
            annot=True,
            fmt='d',
            cmap='Blues',
            xticklabels=self.target_classes,
            yticklabels=self.target_classes
        )
        plt.xlabel('Predicted')
        plt.ylabel('Actual')
        plt.title(f'Confusion Matrix - Epoch {epoch + 1}')
        plt.show()


class ConfidenceGraph:
    """
    Class to plot accuracy and confidence over training epochs.
    """

    def __init__(self, accuracy: List[float] = None, confidence: List[float] = None):
        """
        Initialize with optional accuracy and confidence data.

        Args:
            accuracy: List of accuracy values per epoch.
            confidence: List of confidence values per epoch.
        """
        self.accuracy = accuracy or []
        self.confidence = confidence or []

    def set_data(self, accuracy: List[float], confidence: List[float]) -> None:
        """
        Set the accuracy and confidence data.

        Args:
            accuracy: List of accuracy values.
            confidence: List of confidence values.
        """
        self.accuracy = accuracy
        self.confidence = confidence

    def plot(self) -> None:
        """
        Plot the accuracy and confidence graph.
        """
        if len(self.accuracy) != len(self.confidence):
            print(f"Data length mis-match: Accuracy:{len(self.accuracy)}, Confidence: {len(self.confidence)}")
            return

        epochs = np.arange(1, len(self.accuracy) + 1)
        plt.figure(figsize=(8, 5))
        plt.plot(epochs, self.accuracy, marker='o', label='Accuracy', color='blue')
        plt.plot(epochs, self.confidence, marker='o', label='Confidence', color='orange')
        plt.axvline(8, linestyle='--', color='gray', alpha=0.5, label='Potential Early Stop')
        plt.title('Accuracy vs Confidence Over Epochs')
        plt.xlabel('Epochs')
        plt.ylabel('Value')
        plt.ylim(0.5, 1.0)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend()
        plt.show()

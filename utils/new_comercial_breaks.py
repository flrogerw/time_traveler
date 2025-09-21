import os
import subprocess
import re
from typing import Any
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
import psycopg2
import pandas as pd
import numpy as np
load_dotenv()

db_config = {
            'dbname': os.getenv("DATABASE"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'host': os.getenv("DB_HOST"),
            'port': os.getenv("DB_PORT"),
        }



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
        df.groupby(['show_id','episode_id'])['break_index']
        .max().reset_index(name='num_breaks')
    )

    stats = breaks_per_ep.groupby('show_id')['num_breaks'].agg(['mean','std']).reset_index()
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
        df.groupby(['show_id','break_index'])['break_midpoint']
        .mean().reset_index(name='avg_break_midpoint')
    )

    # Collect results
    episode_outliers = merged[merged['break_count_outlier']]
    break_outliers = df[df['location_outlier']]

    episode_outlier_breaks = df[df['episode_id'].isin(episode_outliers['episode_id'])][
        ['show_id', 'episode_id', 'break_index', 'break_midpoint']
    ].copy()

    return episode_outliers, break_outliers, avg_midpoints, episode_outlier_breaks


def get_episode_filename(show_id: int) -> list[dict[str, Any]]:
    query = f"""SELECT e.*
                FROM episodes e
                WHERE e.show_id = %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM commercial_breaks cb
                      WHERE cb.media_id = e.episode_id);"""
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, (show_id,))
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        print(f"Database query failed: {e}")
        return []

def insert_commercial_break(episode_id: int, breaks=None):
    if breaks is None:
        breaks = []
    insert_query = f"""INSERT INTO commercial_breaks (media_id, break_point, resume_point) VALUES (%s, %s, %s);"""
    data = [(episode_id, start, end) for start, end in breaks]

    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.executemany(insert_query, data)
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Database query failed: {e}")
        return None

def run_ffmpeg_blackdetect(video_path, duration:float = 0.8, threshold: float = 0.1):
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
    return merge_close_segments(commercials)


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

def format_time(seconds: float) -> str:
    return f"{int(seconds // 3600):02}:{int((seconds % 3600) // 60):02}:{seconds % 60:06.3f}"


def detect_commercials(video_path, start_point: float, end_point: float):
    print(f"Analyzing: {video_path}")
    black = run_ffmpeg_blackdetect(video_path)
    silence = run_ffmpeg_silencedetect(video_path)
    candidates = merge_segments(black, silence)
    candidates = filter_edges(candidates, start_point, end_point)

    return [(round(a, 2), round(b, 2)) for a, b in candidates]


def create_report():
    episode_outliers, break_outliers, avg_breaks, episode_outlier_breaks = analyze_break_outliers(db_config, show_id)



    print("Episodes with abnormal number of breaks:")
    print(episode_outliers[['show_id', 'episode_id', 'num_breaks', 'mean', 'num_breaks_z']])

    episode_outlier_breaks['break_midpoint_fmt'] = episode_outlier_breaks['break_midpoint'].apply(format_time)

    print("\nBreak midpoints for episodes with abnormal number of breaks:")
    print(episode_outlier_breaks)

    print("\nBreaks at unusual locations:")
    print(break_outliers[['show_id', 'episode_id', 'break_index',
                          'break_midpoint', 'avg_midpoint', 'std_midpoint', 'midpoint_z']])

    print("\nAverage break midpoints across episodes:")
    avg_breaks['avg_break_midpoint_fmt'] = avg_breaks['avg_break_midpoint'].apply(format_time)
    print(avg_breaks[['show_id', 'break_index', 'avg_break_midpoint', 'avg_break_midpoint_fmt']])


if __name__ == "__main__":

    REPORT_ONLY = False
    DEV_MODE = False
    #show_ids = [272, 174,271, 274]
    show_ids = [274, 270]

    if not REPORT_ONLY:
        for show_id in show_ids:
            for record in get_episode_filename(show_id):
                year = int(record['episode_airdate'].strftime("%y"))
                decade = f"{(year // 10) % 10}0s"
                video_file = f'{os.getenv("LOCAL_PATH")}/{decade}/{year}/{record["episode_file"]}'
                breaks = detect_commercials(video_file, record['start_point'], record['end_point'])
                if DEV_MODE:
                    for i, (start, end) in enumerate(breaks):
                        print(f"ID: {record['episode_id']}  Segment {i + 1}: {format_time(start)} --> {format_time(end)} ({end - start:.2f} seconds)")
                else:
                    insert_commercial_break(record['episode_id'], breaks)

    if not DEV_MODE or REPORT_ONLY:
        create_report()




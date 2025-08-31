import cv2
import torch
import psycopg2
from PIL import Image
from pgvector import Vector
from psycopg2.extras import RealDictCursor
from transformers import CLIPProcessor, CLIPModel
from pgvector.psycopg2 import register_vector

# --- Configuration ---
DEV_MODE = False
SHOW_ID = 243
SCAN_DURATION_SECONDS = 12
FRAME_INTERVAL_SECONDS = 0.1
SIMILARITY_THRESHOLD = -0.90
PG_CONN_INFO = "dbname=time_traveler host=192.168.1.201 user=postgres password=m06Ar14u"
SCAN_FROM_END = True

# --- Load CLIP model ---
model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")

# --- Setup ---
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

def update_episode_end_time(conn, episode_id: int, end_time: float):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE episodes SET end_point = %s, processed = true WHERE episode_id = %s",
                (end_time, episode_id)
            )
        conn.commit()

        print(f"[UPDATED] Episode ID {episode_id} end_time set to {end_time:.2f}s")
    except Exception as e:
        print(f"[ERROR] Failed to update episode {episode_id}: {e}")
        conn.rollback()


def update_episode_start_time(conn, episode_id: int, start_time: float):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE episodes SET start_point = %s, processed = true WHERE episode_id = %s",
                (start_time, episode_id)
            )
        conn.commit()

        print(f"[UPDATED] Episode ID {episode_id} start_time set to {start_time:.2f}s")
    except Exception as e:
        print(f"[ERROR] Failed to update episode {episode_id}: {e}")
        conn.rollback()


def lowest_end_of_first_segment(timestamps, tolerance=FRAME_INTERVAL_SECONDS * 1.5):
    if not timestamps:
        return None

    segments = []
    start = prev = timestamps[0]

    for t in timestamps[1:]:
        if t - prev <= tolerance:
            prev = t
        else:
            segments.append((start, prev))
            start = prev = t

    # Append the last group
    segments.append((start, prev))

    # Return the first timestamp of the last segment
    return segments[-1][0]

def group_segments(timestamps, tolerance=FRAME_INTERVAL_SECONDS * 1.5):
    if not timestamps:
        return []

    segments = []
    start = prev = timestamps[0]

    for t in timestamps[1:]:
        if t - prev <= tolerance:
            prev = t
        else:
            segments.append((start, prev))
            start = prev = t
    segments.append((start, prev))
    return segments

# --- Connect to PostgreSQL and get episodes ---
conn = psycopg2.connect(PG_CONN_INFO)
register_vector(conn)
cur = conn.cursor(cursor_factory=RealDictCursor)

cur.execute("SELECT * FROM episodes WHERE show_id = %s ORDER BY episode_airdate", (SHOW_ID,))
episodes = cur.fetchall()

if not episodes:
    print(f"No episodes found for show_id = {SHOW_ID}")
    cur.close()
    conn.close()
    exit(0)

none_found = []

counter = 0
for episode in episodes:
    counter +=1
    print(f"{counter} of {len(episodes)}")
    year = episode['episode_airdate'].year
    decade = f"{year % 100 // 10}0s"
    year_short = str(year)[-2:]

    source_path = f"/Volumes/TTBS/time_traveler/{decade}/{year_short}/{episode['episode_file']}"

    print(f"\n--- Processing Episode ID {episode['episode_id']}: {source_path} ---")
    cap = cv2.VideoCapture(source_path)

    if not cap.isOpened():
        print(f"Failed to open video file: {source_path}")
        continue

    fps = cap.get(cv2.CAP_PROP_FPS)
    if fps <= 0:
        print(f"Invalid FPS for video: {source_path}")
        continue

    video_duration = cap.get(cv2.CAP_PROP_FRAME_COUNT) / fps
    total_frames = int(SCAN_DURATION_SECONDS / FRAME_INTERVAL_SECONDS)

    if SCAN_FROM_END:
        start_time = max(0, video_duration - SCAN_DURATION_SECONDS)
    else:
        start_time = 0.0

    matched_timestamps = []

    for i in range(total_frames):
        timestamp = round(start_time + i * FRAME_INTERVAL_SECONDS, 2)
        cap.set(cv2.CAP_PROP_POS_MSEC, timestamp * 1000)
        success, frame = cap.read()
        if not success:
            print(f"Failed to read frame at {timestamp}s")
            continue

        image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        inputs = processor(images=image, return_tensors="pt")
        inputs = {k: v.to(device) for k, v in inputs.items()}
        with torch.no_grad():
            embedding = model.get_image_features(**inputs).squeeze()
            embedding = embedding / embedding.norm(p=2)
            embedding_np = embedding.cpu().numpy()

        cur.execute(
            """
            SELECT id, embedding <#> %s AS distance
            FROM unwanted_frames
            WHERE embedding <#> %s <= %s
            ORDER BY distance ASC
            LIMIT 1
            """,
            (Vector(embedding_np.tolist()), Vector(embedding_np.tolist()), SIMILARITY_THRESHOLD)
        )

        result = cur.fetchone()

        if result:
            hours = int(timestamp) // 3600
            minutes = (int(timestamp) % 3600) // 60
            seconds = int(timestamp) % 60
            matched_id, distance = result
            if DEV_MODE:
                print(f"[MATCH] {timestamp:.1f}s ({hours}:{minutes:02d}:{seconds:02d}) matches ID {result['id']} (distance {result['distance']:.4f})")
            matched_timestamps.append(timestamp)

    cap.release()

    # --- Group and output results ---
    segments = group_segments(matched_timestamps)
    lowest = lowest_end_of_first_segment(matched_timestamps)

    if segments:
        min_start = float("inf")
        max_end = 0.0

        for start, end in segments:
            adjusted_end = round(end + FRAME_INTERVAL_SECONDS, 2)

            if start < min_start:
                min_start = start
            if adjusted_end > max_end:
                max_end = adjusted_end

        if SCAN_FROM_END:
            hours = int(min_start) // 3600
            minutes = (int(min_start) % 3600) // 60
            seconds = int(min_start) % 60
            print(f"\nSetting end_point from: {video_duration:.2f}s to: {min_start:.2f}s ({hours}:{minutes:02d}:{seconds:02d})")
            if not DEV_MODE:
                #update_episode_end_time(conn, episode['episode_id'], lowest)
                update_episode_end_time(conn, episode['episode_id'], min_start)
        else:
            hours = int(max_end) // 3600
            minutes = (int(max_end) % 3600) // 60
            seconds = int(max_end) % 60
            print(f"\nSetting start_point from: 0 to: {max_end:.2f}s ({hours}:{minutes:02d}:{seconds:02d})")
            if not DEV_MODE:
                update_episode_start_time(conn, episode['episode_id'], max_end)

    else:
        print("\nNo unwanted segments detected.")
        if SCAN_FROM_END:
            print(f"Setting end_time to full video duration: {video_duration:.2f}s")
            none_found.append((episode['episode_id'], source_path))
            if not DEV_MODE:
                update_episode_end_time(conn, episode['episode_id'], video_duration)
        else:
            print(f"Setting start_time to 0.00s")
            none_found.append((episode['episode_id'], source_path))
            if not DEV_MODE:
                update_episode_start_time(conn, episode['episode_id'], 0.0)

cur.close()
conn.close()
print(none_found)

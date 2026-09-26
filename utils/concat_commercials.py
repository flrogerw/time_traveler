import os
import re
import logging
import subprocess
from pathlib import Path
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

load_dotenv()
logging.basicConfig(level=logging.INFO)

# ---------------- CONFIG ----------------

OUTPUT_DIR = Path("./output")
OUTPUT_DIR.mkdir(exist_ok=True)

MAX_DURATION = 1200.0   # seconds
GAP_DURATION = 2.0     # seconds
VIDEO_SIZE = "512x384"
FPS = 30

TARGET_I = -26      # Integrated loudness (LUFS)
TARGET_TP = -2   # True peak (dB)
TARGET_LRA = 7    # Loudness range

# ---------------- DB ----------------

def get_db():
    return psycopg.connect(
        dbname=os.getenv("DATABASE"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=5432,
    )

def fetch_clips():
    with get_db() as db:
        with db.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT
                    commercial_id,
                    commercial_file,
                    commercial_start,
                    commercial_end,
                    commercial_airdate
                FROM tmp_commercials
                ORDER BY commercial_airdate, commercial_id
            """)
            return cur.fetchall()

# ---------------- VIDEO UTILS ----------------

def detect_crop(video_path):
    cmd = [
        "ffmpeg",
        "-i", video_path,
        "-t", "5",
        "-vf", "cropdetect=24:16:0",
        "-f", "null",
        "-"
    ]
    result = subprocess.run(cmd, stderr=subprocess.PIPE, text=True)
    matches = re.findall(r"crop=\d+:\d+:\d+:\d+", result.stderr)
    return matches[-1] if matches else None


def trim_crop_scale(clip, out_path):
    crop = detect_crop(clip["commercial_file"])

    # Original scaling/cropping
    vf = f"{crop},scale={VIDEO_SIZE}" if crop else f"scale={VIDEO_SIZE}"

    # Add DAR 4:3
    vf += ",setdar=4/3"

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(clip["commercial_start"]),
        "-to", str(clip["commercial_end"]),
        "-i", clip["commercial_file"],
        "-vf", vf,
        "-r", str(FPS),
        "-c:v", "libx264",
        "-preset", "slow",
        "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        str(out_path),
    ]
    subprocess.run(cmd, check=True)

def generate_gap(out_path):
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", f"color=size={VIDEO_SIZE}:rate={FPS}:duration={GAP_DURATION}:color=black",
        "-f", "lavfi",
        "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
        "-shortest",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        str(out_path),
    ]
    subprocess.run(cmd, check=True)

def concat_files(files, out_file, fps=30, width=512, height=384):
    list_file = out_file.with_suffix(".txt")

    with open(list_file, "w") as f:
        for file in files:
            f.write(f"file '{file.resolve()}'\n")

    vf = (
        f"scale={width}:{height},"
        f"setdar=4/3,"
        f"setsar=1"
    )

    af = (
        f"loudnorm=I={TARGET_I}:TP={TARGET_TP}:LRA={TARGET_LRA},"
        "aresample=async=1"
    )

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(list_file),

        # Video: rebuild timeline
        "-vf", vf,
        "-vsync", "cfr",
        "-r", str(fps),

        # Audio normalization
        "-af", af,

        # Re-encode (THIS is the key)
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-g", str(fps * 2),
        "-keyint_min", str(fps * 2),

        "-c:a", "aac",
        "-ar", "48000",

        # Make MP4 seekable immediately
        "-movflags", "+faststart",

        str(out_file),
    ]

    subprocess.run(cmd, check=True)
    list_file.unlink()

# ---------------- MAIN PROCESS ----------------

def main():
    clips = fetch_clips()

    print(clips)

    # Normalize paths + group by decade
    grouped = {}
    for c in clips:
        decade = (c["commercial_airdate"] // 10) * 10
        c["commercial_file"] = f"/Volumes/TTBS/dump/commercials/{c['commercial_file']}"
        grouped.setdefault(decade, []).append(c)

    with get_db() as db:
        with db.cursor() as cur:

            for decade, decade_clips in grouped.items():
                logging.info(f"Processing {decade}s")

                output_index = 60926
                output_time = 0.0
                batch_duration = 0.0

                batch = []  # {file, duration, id}

                def flush_batch():
                    nonlocal output_index, output_time, batch_duration

                    if not batch:
                        return

                    out_file = OUTPUT_DIR / f"{decade}_{output_index}.mp4"
                    concat_list = []

                    # build concat list with gaps
                    for i, b in enumerate(batch):
                        concat_list.append(b["file"])
                        if i < len(batch) - 1:
                            gap = b["file"].with_name(f"gap_{i}.mp4")
                            generate_gap(gap)
                            concat_list.append(gap)

                    concat_files(concat_list, out_file, fps=30, width=512, height=384)

                    # DB updates
                    cursor = 0.0
                    for b in batch:
                        start = round(cursor, 2)
                        end = round(cursor + b["duration"], 2)

                        cur.execute("""
                            UPDATE tmp_commercials
                            SET commercial_file = %s,
                                commercial_start = %s,
                                commercial_end = %s
                            WHERE commercial_id = %s"""
                        , (
                            out_file.name,
                            start,
                            end,
                            b["commercial_id"],
                        ))

                        cursor = end + GAP_DURATION
                        
                    db.commit()

                    # cleanup
                    for f in concat_list:
                        if f.exists():
                            f.unlink()

                    batch.clear()
                    batch_duration = 0.0
                    output_time = 0.0
                    output_index += 1

                for clip in decade_clips:
                    duration = clip["commercial_end"] - clip["commercial_start"]
                    tmp = OUTPUT_DIR / f"tmp_{clip['commercial_id']}.mp4"

                    trim_crop_scale(clip, tmp)

                    if batch and batch_duration + duration + GAP_DURATION > MAX_DURATION:
                        flush_batch()

                    batch.append({
                        "file": tmp,
                        "duration": duration,
                        "commercial_id": clip["commercial_id"],
                    })
                    batch_duration += duration + (GAP_DURATION if len(batch) > 1 else 0)

                flush_batch()

    logging.info("Done.")

# ---------------- RUN ----------------

if __name__ == "__main__":
    main()

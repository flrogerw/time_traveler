import subprocess
from pathlib import Path

INPUT_DIR = Path("/Volumes/TTBS/dump/sam")
OUTPUT_DIR = Path("/Volumes/TTBS/dump/sam/new")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for avi_file in INPUT_DIR.glob("*.mp4"):
    mp4_file = OUTPUT_DIR / (avi_file.stem + ".mp4")

    cmd = [
        "ffmpeg",
        "-y",                   # overwrite output
        "-i", str(avi_file),
        "-c:v", "libx264",
        "-c:a", "aac",
        "-movflags", "+faststart",
        str(mp4_file),
    ]

    print(f"Converting: {avi_file.name}")
    subprocess.run(cmd, check=True)

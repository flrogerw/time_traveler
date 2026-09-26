from pathlib import Path
import subprocess

INPUT_DIR = Path("/Volumes/ExternalDrive")
OUTPUT_DIR = Path("/Volumes/TTBS/dump/MMC")

OUTPUT_DIR.mkdir(exist_ok=True)

VIDEO_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".m4v",
    ".mpg",
    ".mpeg",
    ".ts",
    ".vob",
}


for video_file in INPUT_DIR.iterdir():
    if video_file.suffix.lower() not in VIDEO_EXTENSIONS:
        continue

    output_file = OUTPUT_DIR / video_file.with_suffix(".mp4").name

    print(f"Processing: {video_file.name}")

    cmd = [
        "ffmpeg",
        "-y",
        "-i", str(video_file),

        "-vsync", "cfr",
        "-r", "30",

        "-vf",
        "scale=512:384,"
        "unsharp=5:5:0.2,"
        "setsar=1,"
        "setdar=4/3",

        "-af", "aresample=async=1",

        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "18",

        "-c:a", "aac",
        "-b:a", "128k",

        "-movflags", "+faststart",

        str(output_file),
    ]
    try:
        subprocess.run(cmd, check=True)
        print(f"Created: {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"Failed: {video_file}")
        print(e)

print("Done.")
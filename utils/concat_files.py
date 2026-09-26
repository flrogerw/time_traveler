import subprocess
from pathlib import Path
import sys

BATCH_SIZE = 10
BLACK_DURATION = 2  # seconds

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def get_video_info(video_file):
    """Return width, height, framerate, audio_rate of a video"""
    # Video stream
    cmd = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height,r_frame_rate",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(video_file)
    ]
    out = subprocess.check_output(cmd, text=True).splitlines()
    width = int(out[0])
    height = int(out[1])
    num, denom = map(int, out[2].split("/"))
    fps = num / denom

    # Audio stream
    cmd_audio = [
        "ffprobe", "-v", "error", "-select_streams", "a:0",
        "-show_entries", "stream=sample_rate",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(video_file)
    ]
    audio_rate = int(subprocess.check_output(cmd_audio, text=True).strip())

    return width, height, fps, audio_rate

def generate_black_clip_for_video(output_file, width, height, fps, audio_rate, duration=BLACK_DURATION):
    """Generate black video + silent audio matching a specific video"""
    cmd = [
        "ffmpeg",
        "-y",
        "-f", "lavfi",
        "-i", f"color=size={width}x{height}:rate={fps}:duration={duration}:color=black",
        "-f", "lavfi",
        "-i", f"anullsrc=channel_layout=stereo:sample_rate={audio_rate}",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-t", str(duration),
        "-c:a", "aac",
        "-shortest",
        str(output_file)
    ]
    subprocess.run(cmd, check=True)

def concat_videos_with_gaps(video_list, output_file):
    """Concatenate videos with 2-second matching black/silent gaps"""
    temp_dir = Path("temp_concat")
    temp_dir.mkdir(exist_ok=True)
    file_list_path = temp_dir / "file_list.txt"

    with open(file_list_path, "w") as f:
        for idx, video in enumerate(video_list):
            f.write(f"file '{video.resolve()}'\n")
            # Add black clip after every video except last
            if idx < len(video_list) - 1:
                black_clip = temp_dir / f"black_{idx}.mp4"
                width, height, fps, audio_rate = get_video_info(video)
                generate_black_clip_for_video(black_clip, width, height, fps, audio_rate)
                f.write(f"file '{black_clip.resolve()}'\n")

    # Concatenate all files with re-encoding
    cmd = [
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",
        "-i", str(file_list_path),
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "192k",
        "-movflags", "+faststart",
        str(output_file)
    ]
    subprocess.run(cmd, check=True)

    # Cleanup temp files
    for temp_file in temp_dir.glob("black_*.mp4"):
        temp_file.unlink()
    file_list_path.unlink()
    temp_dir.rmdir()

def batch_concatenate_with_gaps(input_dir, output_dir, batch_size=BATCH_SIZE):
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    videos = sorted(input_dir.glob("*.mp4"))
    if not videos:
        raise RuntimeError("No MP4 files found")

    for idx, batch in enumerate(chunk_list(videos, batch_size), 1):
        output_file = output_dir / f"batch_{idx}.mp4"
        print(f"Creating {output_file} from {len(batch)} videos...")
        concat_videos_with_gaps(batch, output_file)
        print(f"  → Done")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python batch_concat_slow_motion.py <input_dir> <output_dir>")
        sys.exit(1)

    batch_concatenate_with_gaps(sys.argv[1], sys.argv[2])

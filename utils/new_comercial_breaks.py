import subprocess
import re
from pathlib import Path


def run_ffmpeg_blackdetect(video_path):
    """Extract black screen segments using ffmpeg."""
    cmd = [
        "ffmpeg", "-i", video_path,
        "-vf", "blackdetect=d=1:pix_th=0.10",  # 1 second black min, 10% pixel threshold
        "-an", "-f", "null", "-"
    ]
    result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
    black_segments = []

    for line in result.stderr.splitlines():
        match = re.search(r"black_start:(\d+\.?\d*)\s+black_end:(\d+\.?\d*)", line)
        if match:
            black_segments.append((float(match.group(1)), float(match.group(2))))
    return black_segments


def run_ffmpeg_silencedetect(video_path):
    """Extract silence segments using ffmpeg."""
    cmd = [
        "ffmpeg", "-i", video_path,
        "-af", "silencedetect=n=-50dB:d=1",  # silence = below -50dB for 1 second
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


def format_time(seconds):
    """Convert seconds to HH:MM:SS.sss format."""
    return f"{int(seconds // 3600):02}:{int((seconds % 3600) // 60):02}:{seconds % 60:06.3f}"


def detect_commercials(video_path):
    print(f"Analyzing: {video_path}")
    black = run_ffmpeg_blackdetect(video_path)
    silence = run_ffmpeg_silencedetect(video_path)
    candidates = merge_segments(black, silence)

    print("\n=== Suspected Commercial Breaks ===")
    for i, (start, end) in enumerate(candidates):
        print(f"Segment {i+1}: {format_time(start)} --> {format_time(end)} ({end - start:.2f} seconds)")


if __name__ == "__main__":
    # Replace with your file path
    video_file = "/Volumes/TTBS/time_traveler/80s/80/Soap_Episode_45.mp4"
    detect_commercials(video_file)

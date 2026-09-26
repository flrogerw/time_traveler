import subprocess
import re
import sys
from pathlib import Path

def find_loudest_point(video_path):
    if not Path(video_path).exists():
        raise FileNotFoundError(video_path)

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-nostats",
        "-i", video_path,
        "-map", "0:a:0",
        "-af", "astats=reset=1,ashowinfo",
        "-f", "null",
        "-"
    ]

    process = subprocess.Popen(
        cmd,
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        text=True
    )

    # Audio timestamp from ashowinfo
    time_re = re.compile(r"pts_time:(\d+(?:\.\d+)?)")

    # Peak level formats vary by ffmpeg build
    peak_res = [
        re.compile(r"Peak level dB:\s*(-?\d+(?:\.\d+)?)"),
        re.compile(r"Peak_level:\s*(-?\d+(?:\.\d+)?)"),
        re.compile(r"Overall\.Peak_level=(-?\d+(?:\.\d+)?)"),
    ]

    current_time = None
    loudest_db = -float("inf")
    loudest_time = None

    for line in process.stderr:
        t = time_re.search(line)
        if t:
            current_time = float(t.group(1))

        for peak_re in peak_res:
            p = peak_re.search(line)
            if p and current_time is not None:
                peak = float(p.group(1))
                if peak > loudest_db:
                    loudest_db = peak
                    loudest_time = current_time

    process.wait()

    if loudest_time is None:
        raise RuntimeError("Audio present but no peaks detected")

    return loudest_db, loudest_time

def format_hhmmss(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:06.3f}"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python get_loudest_point.py <video_file>")
        sys.exit(1)

    db, ts = find_loudest_point(sys.argv[1])
    print(f"Loudest peak: {db:.2f} dBFS")
    print(f"Timestamp:   {format_hhmmss(ts)}")

import re
from pathlib import Path

START_RE = re.compile(r'#EXTVLCOPT:start-time=([\d.]+)')
STOP_RE = re.compile(r'#EXTVLCOPT:stop-time=([\d.]+)')
IMAGE_DURATION_RE = re.compile(r'#EXTVLCOPT:image-duration=([\d.]+)')

def parse_m3u_runtime(m3u_path: str) -> float:
    total_seconds = 0.0

    current_start = None
    current_stop = None
    current_image_duration = None

    with open(m3u_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            # Parse options
            if match := START_RE.match(line):
                current_start = float(match.group(1))

            elif match := STOP_RE.match(line):
                current_stop = float(match.group(1))

            elif match := IMAGE_DURATION_RE.match(line):
                current_image_duration = float(match.group(1))

            # Media path line → commit duration
            elif line and not line.startswith("#"):
                if current_image_duration is not None:
                    total_seconds += current_image_duration
                elif current_start is not None and current_stop is not None:
                    total_seconds += (current_stop - current_start)
                else:
                    raise ValueError(f"Missing timing info before media line: {line}")

                # Reset for next entry
                current_start = None
                current_stop = None
                current_image_duration = None

    return total_seconds


def format_runtime(seconds: float) -> str:
    seconds = int(round(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


if __name__ == "__main__":
    m3u_file = "./playlists/TV-CBS-2_playlist.m3u"  # <-- change this
    total = parse_m3u_runtime(m3u_file)

    print(f"Total seconds: {total:.3f}")
    print(f"Total runtime: {format_runtime(total)}")

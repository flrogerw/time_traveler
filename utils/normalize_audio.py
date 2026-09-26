import re
import subprocess
import json
from pathlib import Path
import sys

TARGET_I = -26      # Integrated loudness (LUFS)
TARGET_TP = -2   # True peak (dB)
TARGET_LRA = 7    # Loudness range

def run(cmd):
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    return result.stdout, result.stderr
def analyze_loudness(input_file):
    cmd = [
        "ffmpeg",
        "-i", str(input_file),
        "-af", f"loudnorm=I={TARGET_I}:TP={TARGET_TP}:LRA={TARGET_LRA}:print_format=json",
        "-f", "null",
        "-"
    ]

    _, stderr = run(cmd)

    stats = {}

    patterns = {
        "input_i": re.compile(r"input_i:\s*(-?\d+(?:\.\d+)?)"),
        "input_tp": re.compile(r"input_tp:\s*(-?\d+(?:\.\d+)?)"),
        "input_lra": re.compile(r"input_lra:\s*(-?\d+(?:\.\d+)?)"),
        "input_thresh": re.compile(r"input_thresh:\s*(-?\d+(?:\.\d+)?)"),
        "target_offset": re.compile(r"target_offset:\s*(-?\d+(?:\.\d+)?)"),
    }

    # First try JSON extraction (newer ffmpeg)
    json_block = []
    in_json = False

    for line in stderr.splitlines():
        clean = line.split("] ", 1)[-1].strip()

        if clean == "{":
            in_json = True
            json_block.append("{")
            continue

        if in_json:
            json_block.append(clean)
            if clean == "}":
                try:
                    return json.loads("\n".join(json_block))
                except json.JSONDecodeError:
                    break

    # Fallback: key/value parsing (older / macOS builds)
    for line in stderr.splitlines():
        clean = line.split("] ", 1)[-1]
        for key, regex in patterns.items():
            m = regex.search(clean)
            if m:
                stats[key] = m.group(1)

    if len(stats) != 5:
        raise RuntimeError(
            f"Could not extract loudnorm stats for {input_file}\n"
            f"Parsed keys: {stats.keys()}"
        )

    return stats



def normalize_file(input_file, output_file, stats):
    cmd = [
        "ffmpeg",
        "-y",
        "-i", str(input_file),
        "-af",
        (
            "loudnorm="
            f"I={TARGET_I}:TP={TARGET_TP}:LRA={TARGET_LRA}:"
            f"measured_I={stats['input_i']}:"
            f"measured_TP={stats['input_tp']}:"
            f"measured_LRA={stats['input_lra']}:"
            f"measured_thresh={stats['input_thresh']}:"
            f"offset={stats['target_offset']}:"
            "linear=true"
        ),
        "-map", "0:v",
        "-map", "0:a",
        "-c:v", "copy",
        "-c:a", "aac",
        "-movflags", "+faststart",
        str(output_file)
    ]

    subprocess.check_call(cmd)

def normalize_directory(input_dir, output_dir):
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(input_dir.glob("*.mp4"))
    if not files:
        raise RuntimeError("No MP4 files found")

    for file in files:
        print(f"Processing: {file.name}")

        stats = analyze_loudness(file)
        output_file = output_dir / file.name

        normalize_file(file, output_file, stats)

        print(f"  → normalized")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python normalize_audio_directory.py <input_dir> <output_dir>")
        sys.exit(1)

    normalize_directory(sys.argv[1], sys.argv[2])

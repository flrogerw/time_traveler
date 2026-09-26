import logging
import os
import subprocess
import json
from pathlib import Path
from typing import List, Dict
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

load_dotenv()
logging.basicConfig(level=logging.INFO)

BASE_PATH = Path("/Volumes/TTBS/time_traveler")

def get_db():
    return psycopg.connect(
        dbname=os.getenv("DATABASE"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=5432,
    )

def fetch_commercials(min_id):
    with get_db() as db:
        with db.cursor(row_factory=dict_row) as cur:
            cur.execute(f"""
                SELECT
                    commercial_id,
                    commercial_file,
                    commercial_start,
                    commercial_end,
                    commercial_sponser,
                FROM commercials
                WHERE commercial_id > {min_id}
            """)
            rows = cur.fetchall()
            return [dict(row) for row in rows]


def calculate_path(commercial_file: str) -> Path:
    """
    1970_1 -> decade '70' -> /Volumes/TTBS/time_traveler/70s/commercials/1970_1
    """
    year_part = commercial_file.split("_")[0]
    decade = year_part[-2:]  # '1970' -> '70'
    print(BASE_PATH / f"{decade}s" / "commercials" / commercial_file)
    return BASE_PATH / f"{decade}s" / "commercials" / commercial_file


def analyze_audio_levels(file_path, start, end):
    duration = end - start

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "info",
        "-ss", str(start),
        "-t", str(duration),
        "-i", str(file_path),
        "-af", "volumedetect",
        "-f", "null",
        "-"
    ]

    result = subprocess.run(
        cmd,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True
    )

    mean_volume = None
    max_volume = None

    for line in result.stderr.splitlines():
        line = line.strip()

        if "mean_volume:" in line:
            mean_volume = float(line.split("mean_volume:")[1].replace(" dB", "").strip())

        elif "max_volume:" in line:
            max_volume = float(line.split("max_volume:")[1].replace(" dB", "").strip())

    return {
        "quietest_db": mean_volume,
        "loudest_db": max_volume
    }



def generate_report(data: List[Dict]) -> List[Dict]:
    report = []

    for row in data:
        commercial_id = row["commercial_id"]
        commercial_file = row["commercial_file"]
        start = row["commercial_start"]
        end = row["commercial_end"]
        sponsor = row["commercial_sponser"]

        file_path = calculate_path(commercial_file)

        if not file_path.exists():
            print(f"Missing file: {file_path}")
            continue

        levels = analyze_audio_levels(file_path, start, end)

        report.append({
            "commercial_id": commercial_id,
            "commercial_file": commercial_file,
            "file_path": str(file_path),
            "sponsor": sponsor,
            "quietest_db": levels["quietest_db"],
            "loudest_db": levels["loudest_db"],
        })

    # Sort by quietest section (lowest loudness first)
    report.sort(key=lambda x: (x["quietest_db"] is None, x["quietest_db"]))

    return report


if __name__ == "__main__":
    DATA = fetch_commercials(300)

    report = generate_report(DATA)
    print(json.dumps(report, indent=2))

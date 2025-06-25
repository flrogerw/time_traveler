import subprocess
from typing import List, Tuple

def generate_episode_list(start_end_times: List[Tuple[str, str]]) -> List[Tuple[str, str, str]]:
    """
    Generate a list of episode tuples with start time, end time, and output filename.

    Args:
        start_end_times (list): A list of tuples containing start and end times as strings.

    Returns:
        list: A list of tuples containing start time, end time, and output file names.
    """
    episode_list = []
    episode_number = 46  # Start at Episode 1

    for start, end in start_end_times:
        # Generate filename in 'S01EXX' format
        filename = f"S01E{episode_number:02}.mp4"  # Padded to 2 digits with .mp4 extension
        episode_list.append((start, end, filename))
        episode_number += 1

    return episode_list


def split_video(input_file: str, episodes: List[Tuple[str, str, str]]) -> None:
    """
    Use FFmpeg to split a video into episodes based on start and end times.

    Args:
        input_file (str): Path to the input video file.
        episodes (list): A list of tuples containing start time, end time, and output filename.
    """
    for start, end, filename in episodes:
        try:
            # Calculate duration from start and end times
            start_h, start_m, start_s = map(int, start.split(":"))
            end_h, end_m, end_s = map(int, end.split(":"))
            duration_seconds = (end_h * 3600 + end_m * 60 + end_s) - (start_h * 3600 + start_m * 60 + start_s)

            # FFmpeg command to trim the video
            command = [
                "ffmpeg",
                "-i", input_file,          # Input file
                "-ss", start,              # Start time
                "-t", str(duration_seconds),  # Duration
                "-c:v", "libx264",         # Re-encode video
                "-c:a", "aac",             # Re-encode audio
                "-strict", "experimental", # Compatibility flag
                f"/Volumes/TTBS/dump/speed_racer/{filename}"                   # Output file
            ]

            print(f"Processing {filename} from {start} to {end}...")
            subprocess.run(command, check=True)
            print(f"Created {filename} successfully.")

        except Exception as e:
            print(f"Error processing {filename}: {e}")


if __name__ == "__main__":
    # Define input video file and start/end times
    input_video = "/Volumes/TTBS/dump/speed_racer/SPEED_RACER_COMPLETE_SERIES_D6.mp4"
    start_end_times = [
        ("00:00:00", "00:23:33"),  # Episode 1
        ("00:23:33", "00:47:00"),  # Episode 2
        ("00:47:00", "01:10:36"),  # Episode 3
        ("01:10:36", "01:32:57"),  # Episode 4
        ("01:32:57", "01:56:09"),  # Episode 5
        ("01:56:09", "02:19:44"),  # Episode 6
        ("02:19:44", "02:42:57"),  # Episode 7
       # ("02:49:41", "03:13:25"),  # Episode 8
       # ("03:13:25", "03:37:15"),  # Episode 9
    ]

    # Generate episode list
    episodes = generate_episode_list(start_end_times)
    print(episodes)

    # Split video using FFmpeg
    split_video(input_video, episodes)

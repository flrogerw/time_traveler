import subprocess


def is_video_black_and_white(video_path):
    """
    Determines if a video is black and white using FFmpeg.

    Args:
        video_path (str): Path to the video file.

    Returns:
        bool: True if the video is black and white, False otherwise.
    """
    command = [
        "ffmpeg", "-i", video_path,
        "-vf", "format=gray,signalstats",
        "-f", "null", "-"
    ]
    try:
        result = subprocess.run(command, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        output = result.stderr  # FFmpeg writes analysis logs to stderr

        # Check for non-zero chroma signals in output
        if "U:" in output or "V:" in output:
            for line in output.splitlines():
                if "U:" in line or "V:" in line:
                    chroma_values = [float(val.split(":")[-1]) for val in line.split() if val.startswith(("U:", "V:"))]
                    if any(value > 0.0 for value in chroma_values):
                        return False  # Color detected
        return True  # No chroma detected, likely black and white

    except FileNotFoundError:
        raise FileNotFoundError("FFmpeg is not installed or not in the system PATH.")


# Example usage
video_file = "/Volumes/TTBS/time_traveler/80s/81/WKRP_Straight_from_the_Heart.mp4"
try:
    is_bw = is_video_black_and_white(video_file)
    if is_bw:
        print("The video is black and white.")
    else:
        print("The video is in color.")
except FileNotFoundError as e:
    print(e)

import os
import subprocess

PIC_DIR = '/Volumes/shared/dump/pic_dump/'


def get_video_duration(file_path):
    # Use FFmpeg to get the video duration
    command = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        file_path
    ]

    try:
        # Capture the duration of the video
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return float(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        print(f"Error getting duration of {file_path}: {e}")
        return None


def process_video(file_path):
    # Get the duration of the video
    duration = get_video_duration(file_path)
    if duration is None or duration < 10:  # Ensure video is at least 10 seconds long
        print(f"Skipping {file_path}: duration less than 10 seconds or error retrieving duration.")
        return

    # Extract one frame per second for the first 5 seconds
    first_frames_command = [
        "ffmpeg",
        "-ss", "0",
        "-t", "5",
        "-i", file_path,
        "-vf", "fps=1",
        os.path.join(os.path.dirname(file_path), f"{PIC_DIR}first_5_seconds_{os.path.basename(file_path)}_%03d.png")
    ]

    # Extract one frame per second for the last 5 seconds
    last_frames_command = [
        "ffmpeg",
        "-sseof", "-5",  # Start 5 seconds from the end
        "-t", "5",
        "-i", file_path,
        "-vf", "fps=1",
        os.path.join(os.path.dirname(file_path), f"{PIC_DIR}last_5_seconds_{os.path.basename(file_path)}_%03d.png")
    ]

    try:
        # Run the FFmpeg commands
        subprocess.run(first_frames_command, check=True)
        subprocess.run(last_frames_command, check=True)
        print(f"Processed: {file_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing {file_path}: {e}")


def traverse_and_process(directory):
    # Recursively traverse the directory
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.mp4'):
                file_path = os.path.join(root, file)
                process_video(file_path)


if __name__ == "__main__":
    # Replace this with the path to the directory you want to traverse
    target_directory = "/Volumes/shared/time_traveler/80s/88"
    traverse_and_process(target_directory)

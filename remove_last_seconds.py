import os
import subprocess

# Directory containing your mp4 files
directory = "/Volumes/shared/dump/joanieloveschachicompletedvdrip"

# Function to get the duration of the video
def get_video_duration(filepath):
    cmd = ['ffmpeg', '-i', filepath, '-hide_banner']
    result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    for line in result.stderr.splitlines():
        if "Duration" in line:
            time_str = line.split("Duration: ")[1].split(",")[0]
            h, m, s = map(float, time_str.split(":"))
            duration_in_seconds = h * 3600 + m * 60 + s
            return duration_in_seconds
    return None

# Function to trim the last 4 seconds
def trim_video(filepath, output_path):
    duration = get_video_duration(filepath)
    if duration is None:
        print(f"Could not get duration for {filepath}")
        return

    # Trim the last 4 seconds
    trim_duration = duration - 4.0
    cmd = [
        'ffmpeg', '-i', filepath, '-t', str(trim_duration),
        '-c', 'copy', output_path
    ]
    subprocess.run(cmd)

# Iterate through all mp4 files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".mp4"):
        input_filepath = os.path.join(directory, filename)
        output_filepath = os.path.join(directory, f"trimmed_{filename}")
        print(f"Processing {filename}")
        trim_video(input_filepath, output_filepath)
        print(f"Saved trimmed file as {output_filepath}")

print("Processing complete.")

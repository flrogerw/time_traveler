import os
import subprocess

directory = '/Volumes/TTBS/dump/hard_castle'
video_extensions = '.mp4'

def get_aspect_ratio(file_path):
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        file_path
    ]
    try:
        output = subprocess.check_output(cmd).decode().splitlines()
        width, height = map(int, output)
        print(width, height)
        return round(width / height, 2)
    except Exception as e:
        print(f"Error with {file_path}: {e}")
        return None

for filename in os.listdir(directory):
    if filename.lower().endswith(video_extensions):
        full_path = os.path.join(directory, filename)
        ratio = get_aspect_ratio(full_path)
        print(ratio)
        if ratio is None:
            continue
        if abs(ratio - 4.3) > 0.01:  # Allowing for rounding error
            print(f"Deleting: {filename} (Aspect Ratio: {ratio})")
            #os.remove(full_path)
        else:
            print(f"Keeping: {filename} (Aspect Ratio: {ratio})")

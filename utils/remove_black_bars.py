import glob
import os
import subprocess
import re
import hashlib
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse


def get_video_resolution(file_path):
    # Use ffprobe to get the resolution (width x height)
    command = [
        'ffprobe', '-v', 'error', '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height', '-of', 'csv=p=0:s=x', file_path
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    resolution = result.stdout.strip()
    return resolution


def get_audio_loudness(file_path):
    # Use ffmpeg to analyze the loudness (Integrated LUFS)
    command = [
        'ffmpeg', '-i', file_path, '-filter_complex', 'ebur128', '-f', 'null', '-'
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    counter = {}
    for line in result.stderr.splitlines():
        match = re.search(r'I:\s*(-?\d+\.\d+)\s*LUFS', line)
        if match:
            hash_str = hashlib.md5(str(match.group(1)).encode()).hexdigest()
            if hash_str in counter:
                counter[hash_str].append(match.group(1))
            else:
                counter[hash_str] = [match.group(1)]

    largest_list_key = max(counter, key=lambda k: len(counter[k]))
    return counter[largest_list_key][0]


def process_video(filepath):
    temp_file = filepath.replace('.mp4', '_tmp.mp4')
    result = subprocess.run(
        ["ffmpeg", "-i", filepath, "-vf", 'scale=640:480,setdar=4/3', '-af', 'loudnorm=I=-26:TP=-2:LRA=7',
         f"{temp_file}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    Path(temp_file).rename(filepath)
    return str(result.stdout)


def process_remove_bars(input_file):
    a = urlparse(input_file)
    file = Path(os.path.basename(a.path))
    filename = str(file.with_suffix(''))
    parent_path = Path(input_file).parent

    cropdetect_cmd = [
        'ffmpeg', '-i', input_file, '-vf', 'cropdetect', '-f', 'null', '-'
    ]
    result = subprocess.run(cropdetect_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    counter = {}
    for line in result.stderr.splitlines():
        match = re.search(r'crop=(\d+:\d+:\d+:\d+)', line)
        if match:
            hash_str = hashlib.md5(str(match.group(1)).encode()).hexdigest()
            if hash_str in counter:
                counter[hash_str].append(match.group(1))
            else:
                counter[hash_str] = [match.group(1)]

    largest_list_key = max(counter, key=lambda k: len(counter[k]))
    crop_values = counter[largest_list_key].pop()

    if crop_values:
        print(f"Detected crop values: {crop_values}")

        output_file = f"{parent_path}/{filename}.build.mp4"

        crop_cmd = [
            'ffmpeg', '-loglevel', 'quiet', '-i', input_file,
            '-vf', f'crop={crop_values},scale=640:480,setdar=4/3',
            '-af', 'loudnorm = I = -26:TP = -2:LRA = 7',
            '-b:v', '1500k',
            '-c:v', 'h264_videotoolbox',
            '-c:a', 'aac',
            '-b:a', '128k',
            output_file
        ]

        subprocess.run(crop_cmd)
        Path(output_file).rename(input_file.replace(' ', '_'))
        print(f"Video cropped and saved as: {output_file}")
    else:
        print("Crop values could not be detected.")


def main():
    files_to_process = []
    for file in glob.glob(f"/Volumes/shared/time_traveler/60s/66/*.mp4"):  #

        files_to_process.append(file)

    for i, file in enumerate(files_to_process):
        print(f"{i + 1} of {len(files_to_process)}: {file}")
        start_time = datetime.now()

        #process_video(file)
        resolution = get_video_resolution(file)
        loudness = get_audio_loudness(file)
        print(f"{resolution} {loudness}")
        if resolution == '640x480' and -27 <= float(loudness) <= -25:
            continue

        print('PROCESS')
        #process_remove_bars(file)

        end_time = datetime.now()
        final_time = end_time - start_time
        total_seconds = int(final_time.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"{i + 1} of {len(files_to_process)}  {hours:02}:{minutes:02}:{seconds:02}")

if __name__ == '__main__':
    main()

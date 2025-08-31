import glob
import os
import subprocess
import re
import hashlib
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ProcessPoolExecutor


def get_video_resolution(file_path):
    try:
        # Use ffprobe to get the resolution (width x height)
        command = [
            'ffprobe', '-v', 'error', '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height', '-of', 'csv=p=0:s=x', file_path
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        resolution = result.stdout.strip()
        return resolution
    except Exception as e:
        print(f"Error getting video resolution for {file_path}: {e}")
        return None


def get_audio_loudness(file_path):
    try:
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
    except Exception as e:
        print(f"Error getting audio loudness for {file_path}: {e}")
        return None


def process_remove_bars(input_file):
    try:
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
                '-af', 'loudnorm=I=-26:TP=-2:LRA=7',
                '-b:v', '1500k',
                '-c:v', 'h264_videotoolbox',
                '-c:a', 'aac',
                '-b:a', '128k',
                output_file
            ]

            subprocess.run(crop_cmd)
            Path(output_file).rename(f"{input_file}_x".replace(' ', '_'))
            print(f"Video cropped and saved as: {output_file}")
        else:
            print("Crop values could not be detected.")
    except Exception as e:
        print(f"Error processing {input_file}: {e}")


def process_file(file):
    try:
        print(f"Processing file: {file}")
        start_time = datetime.now()

        """
                resolution = get_video_resolution(file)
                if resolution is None:
                    print(f"Skipping {file} due to resolution error.")
                    return

                loudness = get_audio_loudness(file)
                if loudness is None:
                    print(f"Skipping {file} due to loudness error.")
                    return

                if resolution == '640x480' and -27 <= float(loudness) <= -25:
                    print(f"Skipping {file} (already processed with acceptable resolution and loudness)")
                else:
        """

        process_remove_bars(file)

        end_time = datetime.now()
        final_time = end_time - start_time
        total_seconds = int(final_time.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"Processed {file} in {hours:02}:{minutes:02}:{seconds:02}")

    except Exception as e:
        print(f"Error processing file {file}: {e}")


def main():
    files_to_process = glob.glob(f"/Volumes/TTBS/time_traveler/90s/97/Bill_Nye_Life_Cycles.mp4")
    print(len(files_to_process))

    with ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(process_file, files_to_process)


if __name__ == '__main__':
    main()

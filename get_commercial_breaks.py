import math
import os
import subprocess

result_times = []
result_times_str = []

logfile = os.path.join('./', "FFMPEGLOG.txt")
input_file = '/Volumes/shared/time_traveler/60s/commercials/1960s_32_48.mp4'

def seconds_to_mm_ss(seconds):
    minutes = seconds // 60
    remaining_seconds = seconds % 60
    return f"{minutes:02}:{remaining_seconds:02}"

def get_video_duration(file_path):
    result = subprocess.run(
        ['ffprobe', '-i', file_path, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv=p=0'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    return int(math.floor(float(result.stdout.decode('utf-8').strip())))

duration = get_video_duration(input_file)
print(f"Video duration: {duration} seconds")

subprocess.call(
    f'ffmpeg -i {input_file} -vf "blackdetect=d=1.5:pix_th=0.05" -an -f null - 2>&1 | grep blackdetect > {logfile}',
    shell=True)
logfile = logfile.replace("\ ", " ")
start = 0
with open(logfile, 'r') as log_file:
    row = log_file.readline()  # Read the first line
    while row != '':  # EOF is an empty string
        if 'black_start' in row:
            deltas = row.split("\n")[0:1][0].split(' ')[3:]
            end = math.ceil(float(deltas[0].split(':')[1]))
            result_times.append((start, end))
            result_times_str.append((seconds_to_mm_ss(start), seconds_to_mm_ss(end), end - start))
            start = math.floor(float(deltas[1].split(':')[1]))
        row = log_file.readline()  # Move to the next line after processing
    result_times.append((start, duration))
print(len(result_times))
print(result_times)
print(result_times_str)
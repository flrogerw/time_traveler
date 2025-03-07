#!/usr/bin/python3
import math
import os
import subprocess
from pathlib import Path
import glob
import psycopg2

### FILE NAMES LOOK LIKE:  1977_hygiene_super-strength-clearasil.mp4
conn = psycopg2.connect(database="time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.201", port=5432)
cur = conn.cursor()

def get_video_duration(file_path):
    result = subprocess.run(
        ['ffprobe', '-i', file_path, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv=p=0'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    return int(math.floor(float(result.stdout.decode('utf-8').strip())))


def get_breaks(file_path):
    logfile = os.path.join('./', "FFMPEGLOG.txt")
    duration = get_video_duration(file_path)
    result_times = []
    subprocess.call(
        f'ffmpeg -i {file_path} -vf "blackdetect=d=1.5:pix_th=0.05" -an -f null - 2>&1 | grep blackdetect > {logfile}',
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
                start = math.floor(float(deltas[1].split(':')[1]))
            row = log_file.readline()  # Move to the next line after processing
        result_times.append((start, duration))
    return result_times


def config_entry(file_path):
    file = Path(os.path.basename(file_path))
    filename = str(file.with_suffix(''))
    filename = filename.split('!')[0]
    year, type, sponsor = filename.split('_')
    sponsor = sponsor.replace('-', ' ')
    return (int(year), type, sponsor)


def main():
    files = list(glob.iglob('/Volumes/shared/time_traveler/70s/commercials/process/*.mp4', recursive=False))
    nFiles = len(files)
    out_file = "/Volumes/shared/time_traveler/70s/commercials/1970s_64_77.mp4"
    processed = []
    sql_final = []

    # Initialize cmd
    cmd = 'ffmpeg '

    # Add inputs
    for f in files:
        cmd += f'-i "{f}" '
        processed.append(config_entry(f))
        if os.path.isdir(f):
            nFiles = nFiles - 1
            continue

    # Add null sound and black image
    cmd += '-vsync 2 -f lavfi -i anullsrc -f lavfi -i "color=c=black:s=640x480:r=25" '
    cmd += '-filter_complex "'
    for i in range(nFiles - 1):
        cmd += '[{}]atrim=duration={}[ga{}];'.format(nFiles, 2, i)
        cmd += '[{}]trim=duration={}[gv{}];'.format(nFiles + 1, 2, i)

    # Merge videos and audios all together
    for i in range(nFiles - 1):
        cmd += '[{}:v][{}:a]'.format(i, i)  # video
        cmd += '[gv{}][ga{}]'.format(i, i)  # gap

    cmd += '[{}:v][{}:a]'.format(nFiles - 1, nFiles - 1)  # last video bit
    cmd += 'concat=n={}:v=1:a=1"'.format(nFiles * 2 - 1)  # Concat video & audio


    # Output file
    cmd += ' -movflags +faststart -y {}'.format(out_file)
    print('Calling the following command: {}'.format(cmd))
    subprocess.call(cmd, shell=True)
    result_times = get_breaks(out_file)
    if len(result_times) == len(processed):
        for i, t in enumerate(processed):
            sql_final.append(processed[i] + result_times[i] + (str(Path(os.path.basename(out_file))),))

        insert_query = """INSERT INTO commercials (
            commercial_airdate, 
            commercial_type, 
            commercial_sponser, 
            commercial_start, 
            commercial_end, 
            commercial_file) 
            VALUES (%s, %s, %s, %s, %s, %s)"""
        # Execute the SQL command for each tuple in the list
        cur.executemany(insert_query, sql_final)
        # Commit the transaction
        conn.commit()
        # Close the cursor and the connection
        cur.close()
        conn.close()
        print("Data inserted successfully.")
    else:
        print('SHIT WENT WRONG')


if __name__ == '__main__':
    main()

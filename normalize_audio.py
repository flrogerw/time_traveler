import os
import subprocess

directory = '/Volumes/shared/time_traveler/60s/commercials/'

for filename in os.listdir(directory):
    if filename.startswith('.'):
        continue
    file_path = os.path.join(directory, filename)

    if os.path.isfile(file_path):
        print(file_path)
        command = [
            'ffmpeg-normalize', file_path,
            '-o', f'/Volumes/shared/time_traveler/60s/commercials/ready/{filename}',
            '-c:a', 'aac',
            '--normalization-type', 'ebu',
            '--keep-loudness-range-target',
            '--target-level', '-26'
        ]

        subprocess.run(command)

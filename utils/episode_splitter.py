import glob
import json
import os
import re
import subprocess
from pathlib import Path
from urllib.parse import urlparse

FILES_DIR = '/Volumes/shared/dump/DeanMartinCelebrityRoast/'
def get_episode_season(url_string):
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    file_name = re.sub(r'^WKRP In Cincinnati ', '', str(file.with_suffix('')), flags=re.IGNORECASE).split('_')[0]
    return file_name[1:3], file_name[-5:].split('-')


def split_file(filepath, start_point, end_point, save_file):
    # print(filepath, start_point, end_point, save_file)
    if end_point is None:
        print(["ffmpeg", "-i", filepath, "-ss", f'{start_point}', '-c', 'copy', f"{save_file}"])
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", filepath, "-ss", f'{start_point}', '-c', 'copy', f"{save_file}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
    else:
        print(["ffmpeg", "-y", "-i", filepath, "-ss", f'{start_point}', '-to', f'{end_point}', '-c', 'copy', f"{save_file}"])
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", filepath, "-ss", f'{start_point}', '-to', f'{end_point}', '-c', 'copy', f"{save_file}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

    #os.remove(filepath)
    # Path(temp_file).rename(filepath)
    return str(result.stdout)


with open('spliter.json', 'r') as file:
    vids = [tuple(item) for item in json.load(file)]
    # (file_path, split_point, start_point, end_point)

    season = "00"
    episodes = 1

for vid in vids:
    # season, episodes = get_episode_season(vid[0])


    save_file = f"{FILES_DIR}roast-S{season.rjust(2, '0')}E{str(episodes).rjust(2, '0')}.mp4"
    split_file(vid[0], vid[1], vid[2], save_file)

    #save_file = f"{FILES_DIR}lost_in_space-S{season.rjust(2, '0')}E{episodes[1].rjust(2, '0')}.mp4"
    #split_file(vid[0], vid[1], None, save_file)
    episodes += 1




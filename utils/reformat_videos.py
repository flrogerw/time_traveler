import re
import os
import glob
import string
import subprocess
from pathlib import Path
from urllib.parse import urlparse


def get_file_name(url_string):
    url_string = re.sub(r"S\d{1,2}E\d{1,2}", '', url_string)
    translator = str.maketrans('', '', string.punctuation)
    a = urlparse(url_string)
    file = Path(os.path.basename(a.path))
    filename = str(file.with_suffix('')).translate(translator).strip()
    filename = re.sub(r'\s+', ' ', filename)
    filename = filename.replace(' ', '_')
    return f"{filename}.mp4"


def rework_file(filepath):
    temp_file = filepath.replace('.mp4', '_tmp.mp4')
    result = subprocess.run(["ffmpeg", "-i", filepath, "-vf", 'scale=640:480,setdar=4/3', f"{temp_file}"],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    #os.remove(filepath)
    Path(temp_file).rename(filepath)
    return str(result.stdout)


def avi_to_mp4(filepath, new_file_path):
    result = subprocess.run(["ffmpeg", "-i", filepath, "-vf", 'scale=640:480,setdar=4/3', f"{new_file_path}"],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    os.remove(filepath)
    # Path(filepath).rename(new_file_path)
    return str(result.stdout)


def listdir_nohidden(path, ext="*"):
    return glob.iglob(f'{path}**/*{ext}', recursive=True)


path = '/Volumes/shared/time_traveler/50s/commercials/raw/*.mp4'
files = [f for f in glob.glob(path)]

for f in files:
    final_file = f"{os.path.dirname(f)}/{get_file_name(f)}"
    print(final_file)
    #avi_to_mp4(f, final_file)
    rework_file(f)

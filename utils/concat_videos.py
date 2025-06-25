#!/usr/bin/python3

import os
import subprocess
import argparse
from pathlib import Path
import glob


def parser():
    parser = argparse.ArgumentParser(
        description='merge videos together with a gap in between')
    parser.add_argument('-fp', '--file_paths', type=str, nargs='+',
                        help='paths of the files to merge separated by ;')
    parser.add_argument('-tg', '--time_gap', type=float,
                        help='time gap between two videos')
    parser.add_argument('-of', '--out_file', type=str,
                        help='name of the output file')
    args = parser.parse_args()
    return args


def main():
    args = parser()
    files = list(glob.iglob('/Volumes/shared/time_traveler/80s/commercials/*.mp4', recursive=False))
    nFiles = len(files)

    # Initialize cmd
    cmd = 'ffmpeg '

    # Add inputs
    for f in files:
        if os.path.isdir(f):
            nFiles = nFiles - 1
            continue

        cmd += f'-i "{f}" '

    # Add null sound and black image
    cmd += '-vsync 2 -f lavfi -i anullsrc -f lavfi -i "color=c=black:s=640x480:r=25" '
    cmd += '-filter_complex "'
    for i in range(nFiles -1):
        cmd += '[{}]atrim=duration={}[ga{}];'.format(nFiles, args.time_gap, i)
        cmd += '[{}]trim=duration={}[gv{}];'.format(nFiles + 1, args.time_gap, i)

    # Merge videos and audios all together
    for i in range(nFiles -1):
        cmd += '[{}:v][{}:a]'.format(i, i)  # video
        cmd += '[gv{}][ga{}]'.format(i, i)  # gap

    cmd += '[{}:v][{}:a]'.format(nFiles - 1, nFiles - 1)  # last video bit
    cmd += 'concat=n={}:v=1:a=1"'.format(nFiles * 2 - 1)  # Concat video & audio

    # Output file
    cmd += ' {}'.format(args.out_file)
    print('Calling the following command: {}'.format(cmd))
    subprocess.call(cmd, shell=True)


if __name__ == '__main__':
    main()

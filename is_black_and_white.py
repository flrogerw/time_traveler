import subprocess

import cv2
import numpy as np


def is_bw_frame(frame, tolerance=5):
    return np.all(np.abs(frame[:,:,0] - frame[:,:,1]) < tolerance) and \
           np.all(np.abs(frame[:,:,1] - frame[:,:,2]) < tolerance)

def is_video_black_and_white_opencv(video_path, max_samples=20):
    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    step = max(total_frames // max_samples, 1)
    count_bw = 0
    for i in range(0, total_frames, step):
        cap.set(cv2.CAP_PROP_POS_FRAMES, i)
        ret, frame = cap.read()
        if not ret:
            continue
        if is_bw_frame(frame):
            count_bw += 1
    cap.release()
    return count_bw >= max_samples * 0.95  # 95% of sampled frames are grayscale

# Example usage
video_file = "/Volumes/TTBS/time_traveler/80s/85/Airwolf_Eruption.mp4"
try:
    is_bw = is_video_black_and_white_opencv(video_file)
    if is_bw:
        print("The video is black and white.")
    else:
        print("The video is in color.")
except FileNotFoundError as e:
    print(e)

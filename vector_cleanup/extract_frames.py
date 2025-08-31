import cv2
import os

def extract_frames(video_path, output_folder, start_time=0.0, duration=10.0, interval=0.1):
    """
    Extract frames from a video at every `interval` seconds within a given time window.

    :param video_path: Path to the video file.
    :param output_folder: Folder to save extracted frames.
    :param start_time: Start time in seconds.
    :param duration: Duration in seconds to capture frames from start_time.
    :param interval: Interval in seconds between frames.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"Error: Cannot open video file {video_path}")
        return

    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    video_length = total_frames / fps

    end_time = min(start_time + duration, video_length)
    frame_interval = int(fps * interval)

    start_frame = int(start_time * fps)
    end_frame = int(end_time * fps)

    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

    current_frame = start_frame
    frame_count = 0

    while current_frame <= end_frame:
        ret, frame = cap.read()
        if not ret:
            break

        # Convert to grayscale
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

        # Calculate non-black pixel ratio
        non_black_pixels = cv2.countNonZero(gray)
        total_pixels = gray.size
        non_black_ratio = non_black_pixels / total_pixels

        # Skip mostly black frames
        BLACK_FRAME_THRESHOLD = 0.0000005  # Allow only 5% non-black pixels
        if non_black_ratio < BLACK_FRAME_THRESHOLD:
            print(f"Skipping mostly black frame at {current_frame / fps:.2f}s ({non_black_ratio:.4f})")
        else:
            frame_filename = os.path.join(output_folder, f"frame_{frame_count:04d}.jpg")
            cv2.imwrite(frame_filename, frame)
            frame_count += 1

        frame_count += 1
        current_frame += frame_interval
        cap.set(cv2.CAP_PROP_POS_FRAMES, current_frame)

    cap.release()
    print(f"Extracted {frame_count} frames to {output_folder}")


if __name__ == "__main__":
    video_file = "/Volumes/TTBS/time_traveler/90s/92/Baywatch_Reunion.mp4"
    output_dir = "unwanted_frames"
    extract_frames(video_file, output_dir, start_time=2614.7, duration=8, interval=0.05)

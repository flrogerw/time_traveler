import subprocess
import sys
import os

DEFAULT_FILE_LOCATION = "/Volumes/TTBS/dump/thebeatlescartoon_201910/"


def process_video(input_file, crop_point, prepend_file, output_file):
    """
    Crops the input video, prepends another video, and adds a black screen in between.
    """
    try:
        # Ensure files exist
        for file in [DEFAULT_FILE_LOCATION + input_file, DEFAULT_FILE_LOCATION + prepend_file]:
            if not os.path.isfile(file):
                print(f"Error: File '{file}' does not exist.")
                return

        # Step 1: Crop the original video
        cropped_file = "cropped_video.mp4"
        crop_command = [
            "ffmpeg",
            "-i", DEFAULT_FILE_LOCATION + input_file,
            "-ss", crop_point,  # Start point
            "-c:v", "libx264",
            "-c:a", "aac",
            "-crf", "23",
            "-preset", "fast",
            "-y",
            DEFAULT_FILE_LOCATION + cropped_file
        ]
        subprocess.run(crop_command, check=True)

        # Step 2: Create a black screen with silent audio
        black_screen_file = "black_screen.mp4"

        # Step 3: Concatenate the videos
        concat_command = [
            "ffmpeg",
            "-i", DEFAULT_FILE_LOCATION + prepend_file,
            "-i", DEFAULT_FILE_LOCATION + black_screen_file,
            "-i", DEFAULT_FILE_LOCATION + cropped_file,
            "-filter_complex", "[0:v:0][0:a:0][1:v:0][1:a:0][2:v:0][2:a:0]concat=n=3:v=1:a=1[outv][outa]",
            "-map", "[outv]",
            "-map", "[outa]",
            "-c:v", "libx264",
            "-c:a", "aac",
            "-crf", "18",
            "-y",
            DEFAULT_FILE_LOCATION + output_file
        ]
        subprocess.run(concat_command, check=True)
        print(f"Output saved to: {DEFAULT_FILE_LOCATION}{output_file}")

        # Clean up temporary files
        os.remove(DEFAULT_FILE_LOCATION + cropped_file)

    except subprocess.CalledProcessError as e:
        print(f"Error processing video: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python batch_process_videos.py <batch_file>")
        print("The batch file should contain lines in the format:")
        print("<input_file> <crop_point> <prepend_file>")
        print("Example line: video.mp4 00:01:45 intro.mp4")
        sys.exit(1)

    batch_file = sys.argv[1]

    if not os.path.isfile(batch_file):
        print(f"Error: Batch file '{batch_file}' does not exist.")
        sys.exit(1)

    with open(batch_file, "r") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):  # Skip empty lines and comments
                continue

            try:
                input_file, crop_point, prepend_file = line.split()
                name, ext = os.path.splitext(input_file)
                output_file = f"{name}_processed{ext}"
                process_video(input_file, crop_point, prepend_file, output_file)
            except ValueError:
                print(f"Invalid line format: {line}")

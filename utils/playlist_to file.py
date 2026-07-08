import re
import subprocess


def parse_playlist(playlist):
    # Regular expressions to match start-time, stop-time, and file paths
    start_time_pattern = re.compile(r'#EXTVLCOPT:start-time=(\d+)')
    stop_time_pattern = re.compile(r'#EXTVLCOPT:stop-time=(\d+)')
    file_path_pattern = re.compile(r'^/.*\.mp4$')

    # To hold the segments for ffmpeg
    segments = []

    # Temporary variables for start and stop times
    start_time = None
    stop_time = None

    # Process each line in the playlist
    for line in playlist.splitlines():
        if start_time_match := start_time_pattern.match(line):
            start_time = int(start_time_match.group(1))
        elif stop_time_match := stop_time_pattern.match(line):
            stop_time = int(stop_time_match.group(1))
        elif file_path_match := file_path_pattern.match(line):
            file_path = file_path_match.group(0)
            if start_time is not None and stop_time is not None:
                segments.append((file_path, start_time, stop_time))
                # Reset start and stop times for the next segment
                start_time, stop_time = None, None

    return segments


def generate_ffmpeg_command(segments, output_file):
    # Create the FFmpeg command using the parsed segments
    ffmpeg_command = ["ffmpeg"]

    # Append each input segment with start and stop times
    for file_path, start, stop in segments:
        ffmpeg_command += ["-ss", str(start), "-to", str(stop), "-i", file_path]

    # Remove metadata and reset timestamps
    ffmpeg_command += ["-map_metadata", "-1", "-reset_timestamps", "1"]

    # Create the filter_complex string for concatenation
    filter_complex = f"[0:v][0:a]concat=n={len(segments)}:v=1:a=1[outv][outa]"
    ffmpeg_command += ["-filter_complex", filter_complex, "-map", "[outv]", "-map", "[outa]", "-y", output_file]

    return ffmpeg_command


def run_ffmpeg_command(command):
    # Run the FFmpeg command using subprocess
    try:
        subprocess.run(command, check=True)
        print(f"FFmpeg command executed successfully: {' '.join(command)}")
    except subprocess.CalledProcessError as e:
        print(f"Error running FFmpeg: {e}")


def read_playlist_from_file(filename):
    # Read the playlist from a file
    with open(filename, 'r') as file:
        playlist = file.read()
    return playlist


# Example usage
playlist_file = '/Volumes/shared/time_traveler/sys/playlists/TV-ABC-7_playlist.m3u'  # Path to the playlist file

# Read playlist from the file
playlist = read_playlist_from_file(playlist_file)

# Parse the playlist
segments = parse_playlist(playlist)

# Generate the FFmpeg command
output_file = "output.mp4"  # Name of the output file
ffmpeg_command = generate_ffmpeg_command(segments, output_file)

# Run the FFmpeg command
run_ffmpeg_command(ffmpeg_command)

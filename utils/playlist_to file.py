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


def generate_ffmpeg_command(segments, output_file, width=512, height=384, fps=30):
    cmd = ["ffmpeg", "-y"]

    # Inputs
    for file_path, start, stop in segments:
        cmd += [
            "-ss", str(start),
            "-to", str(stop),
            "-i", file_path
        ]

    filter_parts = []
    concat_inputs = []

    for i in range(len(segments)):
        filter_parts.append(
            f"[{i}:v]"
            f"scale={width}:{height}:flags=bicubic,"
            f"fps={fps},"
            f"setsar=1,"
            f"setdar=4/3,"
            f"format=yuv420p"
            f"[v{i}]"
        )
        filter_parts.append(
            f"[{i}:a]"
            f"aresample=48000"
            f"[a{i}]"
        )
        concat_inputs.append(f"[v{i}][a{i}]")

    filter_complex = (
        ";".join(filter_parts)
        + ";"
        + "".join(concat_inputs)
        + f"concat=n={len(segments)}:v=1:a=1[outv][outa]"
    )

    cmd += [
        "-filter_complex", filter_complex,
        "-map", "[outv]",
        "-map", "[outa]",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-profile:v", "high",
        "-level", "3.1",
        "-r", str(fps),
        "-c:a", "aac",
        "-movflags", "+faststart",
        output_file
    ]

    return cmd




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
STATIONS2 = ['TV-CBS-2','TV-NBC-4','TV-SYN-5','TV-ABC-7','TV-SYN-9','TV-SYN-11','TV-SYN-13']
STATIONS = ['TV-ABC-7']
for station in STATIONS:
    playlist_file = f'../playlists/{station}_playlist.m3u'  # Path to the playlist file

    # Read playlist from the file
    playlist = read_playlist_from_file(playlist_file)

    # Parse the playlist
    segments = parse_playlist(playlist)

    # Generate the FFmpeg command
    output_file = f"output_{station}.mp4"  # Name of the output file
    ffmpeg_command = generate_ffmpeg_command(segments, output_file)

    # Run the FFmpeg command
    run_ffmpeg_command(ffmpeg_command)

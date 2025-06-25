import subprocess
import os

# Input and output file paths
input_file = '/Volumes/shared/time_traveler/60s/commercials/1960s_116_143.mp4'
temp_file = '/Volumes/shared/time_traveler/60s/commercials/temp_output.mp4'
final_output_file = '/Volumes/shared/time_traveler/60s/commercials/output.mp4'

# FFmpeg filter_complex command with multiple trims and black inserts
ffmpeg_command = [
    'ffmpeg', '-y', '-i', input_file, '-filter_complex',

    # First segment: 0 to 654 seconds (before the cut)
    "[0:v]trim=0:496,setpts=PTS-STARTPTS[v1]; "  # 0 to 654 seconds (video)
    "[0:a]atrim=0:496,asetpts=PTS-STARTPTS[a1]; "  # 0 to 654 seconds (audio)

    # Second segment: 721 seconds to the end (after the cut)
    "[0:v]trim=653,setpts=PTS-STARTPTS[v2]; "  # 721 seconds to the end (video)
    "[0:a]atrim=653,asetpts=PTS-STARTPTS[a2]; "  # 721 seconds to the end (audio)

    # Insert black screen for 2 seconds (640x480 resolution)
    "color=c=black:s=640x480:d=2[black]; "  # 2-second black screen (video)
    "anullsrc=r=48000:cl=stereo:d=2[black_audio]; "  # 2-second silent audio

    # Concatenate video and audio streams (3 segments)
    "[v1][a1][black][black_audio][v2][a2]concat=n=3:v=1:a=1[outv][outa]",

    # Output mapping to temporary file
    '-map', '[outv]', '-map', '[outa]', temp_file
]


# Execute the initial FFmpeg command using subprocess
subprocess.run(ffmpeg_command)

# Determine the exact end time of the final output
exact_end_time = '1600'  # Replace with the actual end time of the final content

# Final trim to remove any extra padding or black screens
final_trim_command = [
    'ffmpeg', '-i', temp_file,
    '-vf', f"trim=end={exact_end_time},setpts=PTS-STARTPTS",
    '-af', f"atrim=end={exact_end_time},asetpts=PTS-STARTPTS",
    '-map_metadata', '-1',  # Remove original metadata
    '-movflags', '+faststart',  # Optimize file for playback
    '-y', final_output_file
]

# Execute the final trim command using subprocess
subprocess.run(final_trim_command)

# Optional: Remove the temporary file if you don't need it
os.remove(temp_file)

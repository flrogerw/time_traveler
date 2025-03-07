import subprocess

DEFAULT_FILE_LOCATION = '/Volumes/TTBS/dump/king-of-the-hill13seasons/'

concat_command = [
    "ffmpeg",
    "-i", DEFAULT_FILE_LOCATION + 'King of the Hill S06E21.mp4',
    "-i", DEFAULT_FILE_LOCATION + 'black_screen.mp4',
    "-i", DEFAULT_FILE_LOCATION + 'ending.mp4',
    "-filter_complex", "[0:v:0][0:a:0][1:v:0][1:a:0][2:v:0][2:a:0]concat=n=3:v=1:a=1[outv][outa]",
    "-map", "[outv]",
    "-map", "[outa]",
    "-c:v", "libx264",
    "-c:a", "aac",
    "-crf", "18",
    "-y",
          DEFAULT_FILE_LOCATION + 'King of the Hill S06E21x.mp4'
]
subprocess.run(concat_command, check=True)
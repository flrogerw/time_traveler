import subprocess

input_file = "./tmp/Vol_3.mp4"

clips = [
    ("5:35", "27:45"),
    ("27:45", "49:52"),
    ("49:52", "1:12:04"),
]

for idx, (start, end) in enumerate(clips, start=1):
    output_file = f"clip_{idx:03d}.mp4"

    cmd = [
        "ffmpeg",
        "-y",
        "-ss", start,
        "-to", end,
        "-i", input_file,
        "-c", "copy",
        output_file,
    ]

    subprocess.run(cmd, check=True)

print("Done")
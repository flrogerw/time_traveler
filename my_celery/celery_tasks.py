import os
import shutil
import subprocess
from pathlib import Path
from celery import shared_task
from celery.utils.log import get_task_logger

from celery_app import celery_app
from classes.video_utils import VideoReProcess, CommercialBreaks, IsBlackWhite

logger = get_task_logger(__name__)

@celery_app.task(bind=True, name="celery_tasks.is_blackwhite")
def is_blackwhite(self, input_file: str, episode_id: int, dev_mode: bool = True) -> dict:
    try:
        is_bw = IsBlackWhite.is_video_black_and_white_opencv(input_file)
        if not dev_mode:
            IsBlackWhite.insert_bw(episode_id, is_bw)
        return {"episode_id": episode_id, "success": True, "is_bw": is_bw}

    except Exception as e:
        return {"episode_id": episode_id, "success": False, "error": f"is_blackwhite failed {e}"}


@celery_app.task(bind=True, name="celery_tasks.commercial_breaks")
def commercial_breaks(self, input_file: str, episode_id: int, start_point: float, end_point: float,
                      dev_mode: bool = True) -> dict:
    try:
        input_path = Path(input_file)
        black = CommercialBreaks.run_ffmpeg_blackdetect(input_path)
        silence = CommercialBreaks.run_ffmpeg_silencedetect(input_path)
        candidates = CommercialBreaks.merge_segments(black, silence)
        candidates = CommercialBreaks.filter_edges(candidates, start_point, end_point)
        if not dev_mode:
            CommercialBreaks.insert_commercial_break(episode_id, candidates)
        return {"episode_id": episode_id, "success": True}

    except Exception as e:
        return {"episode_id": episode_id, "success": False, "error": "commercial_breaks failed"}

@celery_app.task(bind=True, name="celery_tasks.process_video")
def process_video(self, task_id: int, episode: dict ) -> dict:
    temp_file_name = VideoReProcess.get_random_filename()
    temp_output_file = f"/tmp/meta_{temp_file_name}"
    try:
        input_path = Path(episode['path'])
        metadata = {
            "title": f"{episode['title']} - {episode['airdate']}",
            "artist": episode["showName"],
            "comment": "TV Party Tonight",
            "year": str(episode['airdate'])
        }

        if not input_path.exists():
            logger.error(f"File not found - {episode['path']}")
            return { "success": False, "task": task_id, "error": f"File not found - {episode['path']}"}

        self.update_state(state="PROGRESS", meta={"step": "running ffmpeg", "file": episode['path']})
        VideoReProcess.reprocess(episode['path'], metadata)

    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg error: {e.stderr.decode()}")
        return {"success": False, "task": task_id, "error": "ffmpeg failed"}

    else:
        return {"success": True, "task": task_id, "metadata": metadata}

    finally:
        if os.path.exists(temp_output_file):
            os.remove(temp_output_file)



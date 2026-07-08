import json
import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from celery.celery_tasks import process_video
from dotenv import load_dotenv

load_dotenv()
ROOT_DIR = os.getenv("DIR_ROOT_PI")

db_config = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT"),
}


def update_task_status(update_list: list, status: str = 'working', message=None) -> None:
    if message is None:
        message = {}
    # query = f"""UPDATE video_tasks SET status = %s, message = %s WHERE episode_id = ANY(%s)"""
    query = f"""UPDATE ingestion_tasks SET status = %s, message = %s WHERE task_id = ANY(%s::int[])"""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query, (status, json.dumps(message), update_list))
    conn.commit()
    cur.close()
    conn.close()


def get_db_rows(query: str) -> list[dict]:
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()

    return rows


def get_file_path(airdate, episode_file):
    year = int(airdate.strftime("%y"))
    decade = f"{(year // 10) % 10}0s"
    return f'{ROOT_DIR}/{decade}/{year}/{episode_file}'


def get_video_jobs():
    try:
        query = """
        UPDATE ingestion_tasks
        SET status = 'working'
        WHERE task_id IN (
            SELECT task_id
            FROM ingestion_tasks
            WHERE status = 'pending'
            ORDER BY task_id
            FOR UPDATE SKIP LOCKED
        )
        RETURNING task_id, episode_data
        """

        rows = get_db_rows(query)

        return [
            (row['task_id'], row['episode_data'])
            for row in rows
        ]

    except Exception as e:
        print(f"Database query failed: {e}")
        return []

def make_callback():
    def handle_progress(message):
        if message['status'] == "PROGRESS":
            print(f"Progress:", message['result'])
        elif message['status'] == "FAILURE":
            print(f"Failed:", message)

    return handle_progress


def main_loop(poll_interval=30):
    """Continuously poll for new jobs and submit them to Celery."""
    while True:
        jobs = get_video_jobs()

        if not jobs:
            print("No new jobs, waiting...")
            time.sleep(poll_interval)
            continue

        async_results = []

        # Submit jobs
        for task_id, episode_data in jobs:
            episode_data = json.loads(episode_data)
            print("Submitting:", episode_data["path"], task_id)

            task = process_video.apply_async(
                args=[task_id, episode_data]
            )

            async_results.append((task, episode_data["path"], task_id))

        # Collect results
        for task, video_path, task_id in async_results:
            try:
                result = task.get(timeout=None)  # optional timeout
                status = "complete" if result.get("success") else "error"
                update_task_status(
                    [task_id],
                    status=status,
                    message=result
                )

                print(f"[{video_path}] Final:", result)

            except Exception as e:
                update_task_status(
                    [task_id],
                    status="error",
                    message=str(e)
                )
                print(f"[{video_path}] FAILED:", e)

        time.sleep(poll_interval)


if __name__ == "__main__":
    main_loop(poll_interval=2)  # check DB every 2 min

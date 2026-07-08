from celery import Celery

celery_app = Celery(
    "video_tasks",
    broker="redis://192.168.1.201:6379/0",
    backend="redis://192.168.1.201:6379/1",
)

celery_app.conf.task_routes = {
    "celery_tasks.process_video": {"queue": "video_queue"},
    "celery_tasks.commercial_breaks": {"queue": "video_queue"},
    "celery_tasks.is_blackwhite": {"queue": "video_queue"},
}

celery_app.conf.update(
    broker_transport_options={
        "visibility_timeout": 7200
    },
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_time_limit=7200,
    task_soft_time_limit=7000,
)

import celery_tasks

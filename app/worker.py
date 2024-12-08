from celery import Celery, Task
from celery.exceptions import SoftTimeLimitExceeded
import os
from .ng_toolbox import NGToolbox
from .uploader import TaskUploader
from .db import DBTask
import time
import random

celery = Celery(__name__)
celery.conf.broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
celery.conf.result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")


def check_status(ngw_task_id: str | None, task_type: str, db_task: int | DBTask) -> DBTask:
    task_config = {
        'kpt': {'file_key': 'kpt_file', 'upload_method': TaskUploader.process_file, 'suffix': '.csv'},
        'kad': {'file_key': 'kad_file', 'upload_method': TaskUploader.process_zip, 'suffix': '.zip'},
    }

    if task_type not in task_config:
        raise ValueError(f"Неизвестный тип задачи: {task_type}")

    config = task_config[task_type]
    status_key = f"{task_type}_status"
    task_id = f"{task_type}_task_id"

    max_total_time = 60 * 30  # 30 минут
    start_time = time.time()
    max_delay = 60 * 5  # максимальная задержка между попытками
    attempts = 0

    jitter = random.uniform(0, 3)
    time.sleep(jitter)

    while True:
        if time.time() - start_time > max_total_time:
            raise TimeoutError(f"Превышено время обработки задачи")

        status = NGToolbox.status(task_id=ngw_task_id)

        if status['state'] == 'FAILED':
            raise Exception(status['error'] if status['error'] else 'Неизвестная ошибка')

        if status['state'] == 'CANCELLED':
            raise Exception('Задача была отменена')

        db_task = TaskUploader.create_or_update(
            model=DBTask, instance=db_task, params={status_key: status, task_id: ngw_task_id}
        )

        if status['state'] == 'SUCCESS':
            file_key = config['file_key']
            if not getattr(db_task, file_key, None):
                file_url = status['output'][0]['value']
                file = NGToolbox.download(file_url=file_url)
                task_path = config['upload_method'](
                    content=file,
                    filename=db_task.name + config['suffix'],
                    dest='data/results/',
                    parts=False,
                )

                db_task = TaskUploader.create_or_update(
                    model=DBTask,
                    instance=db_task,
                    params={
                        file_key: task_path[0],
                    },
                )

            break

        jitter = random.uniform(0, 10)
        sleep_time = min(attempts * 2 + jitter, max_delay)
        print(f"Попытка {attempts + 1} через {sleep_time} секунд")
        time.sleep(sleep_time)
        attempts += 1

    return db_task


class CollectKadTask(Task):
    name = 'worker.collect_kad'
    soft_time_limit = 60 * 30  # 30 минут
    time_limit = 60 * 35  # 35 минут
    ignore_result = True

    def before_start(self, task_id, args, kwargs):
        print(f"Задача {task_id} запущена c аргументами {args} и ключевыми аргументами {kwargs}")

    def on_success(self, retval, task_id, args, kwargs):
        print(f"Задача {task_id} завершена")

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        print(f"Задача {task_id} перезапущена")

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print(f"Задача {task_id} завершилась с ошибкой: {exc}")

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        print(f"Задача {task_id} завершила работу")

    def run(self, *args, **kwargs):
        db_task_id = args[0]
        current_stage = 'kpt_status'

        db_task = TaskUploader.create_or_update(
            model=DBTask,
            instance=db_task_id,
        )

        try:
            if os.statvfs('/').f_bsize * os.statvfs('/').f_bavail < 500 * 1024 * 1024:
                raise Exception('Worker (collect_kad): Недостаточно места на диске')

            # ПОЛУЧЕНИЕ СПИСКА КПТ ПО ЗАДАННОЙ ОБЛАСТИ
            if db_task.kpt_task_id:
                db_task = check_status(ngw_task_id=db_task.kpt_task_id, task_type='kpt', db_task=db_task)
            else:
                file_id = NGToolbox.upload(upload_file=db_task.cover_file)
                task_id = NGToolbox.collect_kpt(file_id=file_id)
                db_task = check_status(ngw_task_id=task_id, task_type='kpt', db_task=db_task)

            # ПОЛУЧЕНИЕ ГЕОМЕТРИИ ПО КАДАСТРОВЫМ НОМЕРАМ
            current_stage = 'kad_status'
            if db_task.kad_task_id:
                db_task = check_status(ngw_task_id=db_task.kad_task_id, task_type='kad', db_task=db_task)
            else:
                file_id = NGToolbox.upload(upload_file=db_task.kpt_file)
                task_id = NGToolbox.collect_kad(file_id=file_id)
                db_task = check_status(ngw_task_id=task_id, task_type='kad', db_task=db_task)
        except SoftTimeLimitExceeded:
            TaskUploader.create_or_update(
                model=DBTask,
                instance=db_task,
                params={current_stage: {'state': 'FAILED', 'error': '(Worker): Превышено время выполнения задачи'}},
            )
        except Exception as e:
            TaskUploader.create_or_update(
                model=DBTask, instance=db_task, params={current_stage: {'state': 'FAILED', 'error': str(e)}}
            )
        return


celery.register_task(CollectKadTask())


@celery.task(base=CollectKadTask)
def collect_kad(db_task_id):
    return CollectKadTask().run(db_task_id)

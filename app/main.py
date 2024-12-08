import os
import json
from contextlib import asynccontextmanager
from uuid import uuid4
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import aiosqlite
from fastapi import FastAPI, File, UploadFile, Request, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse

from .uploader import TaskUploader, create_archive, delete_paths, execute_db_operations
from .models import TaskModel, ResponseGroupsModel, ResponseTasksModel
from .db import DBTask, DBTasksGroup, create_tables, drop_tables
from app.worker import celery, CollectKadTask


DATABASE_URL = "data/database/database.db"


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    # здесь можно выполнять код при запуске приложения
    print('Запуск приложения. Перезапуск задач.')

    check_folders()
    await create_tables()
    tasks = TaskUploader.get_working_tasks()
    for task in tasks:
        print(f"Перезапуск задачи: {task.name}({task.id})")
        CollectKadTask().apply_async(args=(task.id,), task_id=task.celery_task)
    yield

    # здесь можно выполнять код при остановке приложения
    # await drop_tables()


app = FastAPI(lifespan=app_lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


def check_folders():
    print("Проверка папок")

    folders = [
        'data',
        'data/uploaded',
        'data/results',
        'data/database',
        'data/logs',
        'data/tmp',
    ]

    [os.makedirs(folder, exist_ok=True) for folder in folders]


@app.get("/")
async def home(request: Request):
    return templates.TemplateResponse("index.html", context={"request": request})


@app.get("/groups", status_code=200)
async def get_groups():
    """
    Получение списка задач.
    """
    async with aiosqlite.connect(DATABASE_URL) as db:
        query = """
        SELECT g.id, g.name, g.added, t.kpt_status, t.kad_status
        FROM ngw_task_groups g
        LEFT JOIN ngw_tasks t ON g.id = t.group_id
        """

        cursor = await db.execute(query)
        tasks_rows = await cursor.fetchall()

        groups = {}
        for row in tasks_rows:
            group_id, name, added, kpt_status, kad_status = row

            if group_id not in groups:
                groups[group_id] = {
                    'id': group_id,
                    'name': name,
                    'added': added,
                    'statistics': {
                        'loaded': 0,
                        'in_progress': 0,
                        'completed': 0,
                        'failed': 0,
                        'remaining': 0,
                    },
                }

            stats = groups[group_id]['statistics']

            kpt = json.loads(kpt_status) if kpt_status else None
            kad = json.loads(kad_status) if kad_status else None

            if kpt and kpt['state'] == 'FAILED' or kad and kad['state'] == 'FAILED':
                stats['failed'] += 1
            elif kpt and kpt['state'] == 'SUCCESS' and kad and kad['state'] == 'SUCCESS':
                stats['completed'] += 1
            elif kpt and kpt['state'] != 'PREPARING' or kad and kad['state'] != 'PREPARING':
                stats['in_progress'] += 1

            if kpt or kad:
                stats['loaded'] += 1

        for group in groups.values():
            stats = group['statistics']
            stats['remaining'] = stats['loaded'] - stats['completed'] - stats['failed']
            group['statistics'] = json.dumps(stats, ensure_ascii=False)

        return ResponseGroupsModel(groups=list(groups.values())).dict()


@app.get("/tasks", status_code=200)
async def get_tasks():
    """
    Получение списка задач.
    """

    async with aiosqlite.connect(DATABASE_URL) as db:
        cursor = await db.execute("SELECT id, name, added, kpt_status, kad_status, group_id FROM ngw_tasks")
        tasks = await cursor.fetchall()

        tasks_list = [
            TaskModel(
                id=task[0],
                name=task[1],
                added=task[2],
                kpt_status=task[3],
                kad_status=task[4],
                group_id=task[5],
            )
            for task in tasks
        ]

        return ResponseTasksModel(tasks=tasks_list).dict()


@app.get("/tasks/statistics", status_code=200)
async def get_groups_statistics():
    """
    Получение статистики по группам на основе задач.
    """

    async with aiosqlite.connect(DATABASE_URL) as db:
        cursor = await db.execute("SELECT kpt_status, kad_status FROM ngw_tasks")
        tasks = await cursor.fetchall()

        loaded = len(tasks)
        in_progress = 0
        completed = 0
        failed = 0

        for kpt_status, kad_status in tasks:
            kpt = json.loads(kpt_status) if kpt_status else None
            kad = json.loads(kad_status) if kad_status else None

            if kpt and kpt['state'] == 'FAILED' or kad and kad['state'] == 'FAILED':
                failed += 1
            elif kpt and kpt['state'] == 'SUCCESS' and kad and kad['state'] == 'SUCCESS':
                completed += 1
            elif kpt and kpt['state'] != 'PREPARING' or kad and kad['state'] != 'PREPARING':
                in_progress += 1

        remaining = loaded - completed - failed
        return {
            'loaded': loaded,
            'in_progress': in_progress,
            'completed': completed,
            'failed': failed,
            'remaining': remaining,
        }


@app.get("/groups/{group_id}/download", status_code=200)
async def download_group_files(group_id: int):
    """
    Скачивание файлов группы по ее id.
    """
    os.makedirs('temp', exist_ok=True)
    archive_name = f"data/temp/group_{group_id}_files"

    if os.path.exists(archive_name + '.zip'):
        os.remove(archive_name + '.zip')

    async with aiosqlite.connect(DATABASE_URL) as db:
        query = """
        SELECT t.kpt_file, t.kad_file, g.name
        FROM ngw_task_groups g
        LEFT JOIN ngw_tasks t ON g.id = t.group_id
        WHERE g.id = ?
        """

        cursor = await db.execute(query, (group_id,))
        group_files = await cursor.fetchall()

        files = [file for task in group_files for file in task if file]
        group_path = f'data/temp/group_{group_id}'
        await create_archive(group_path, files, archive_name)

        return FileResponse(path=archive_name + '.zip', filename=f"{group_files[0][2]}_files.zip")


@app.get("/tasks/{task_id}/download", status_code=200)
async def download_task_files(task_id: int):
    """
    Скачивание файлов задачи по ее id.
    """
    os.makedirs('temp', exist_ok=True)
    archive_name = f"data/temp/task_{task_id}_files"

    if os.path.exists(archive_name + '.zip'):
        os.remove(archive_name + '.zip')

    async with aiosqlite.connect(DATABASE_URL) as db:
        cursor = await db.execute("SELECT kpt_file, kad_file, name FROM ngw_tasks WHERE id = ?", (task_id,))
        task_files = await cursor.fetchone()

    task_path = f'data/temp/task_{task_id}'
    await create_archive(task_path, [file for file in task_files if file], archive_name)

    return FileResponse(path=archive_name + '.zip', filename=f"{task_files[2]}_files.zip")


@app.delete("/groups/{group_id}/delete", status_code=200)
async def delete_group(group_id: int):
    async with aiosqlite.connect(DATABASE_URL) as db:
        cursor = await db.execute(
            "SELECT celery_task, kpt_file, kad_file, cover_file FROM ngw_tasks WHERE group_id = ?",
            (group_id,),
        )
        tasks = await cursor.fetchall()

        files_for_delete = []
        for task in tasks:
            if task[0]:
                celery.control.revoke(task[0], terminate=True)
            files_for_delete.extend(task[1:])

        files_for_delete.append(f'data/temp/group_{group_id}_files.zip')
        await delete_paths(*files_for_delete)

        await execute_db_operations(
            db,
            ("DELETE FROM ngw_tasks WHERE group_id = ?", (group_id,)),
            ("DELETE FROM ngw_task_groups WHERE id = ?", (group_id,)),
        )

    return {'message': 'Группа успешно удалена'}


@app.delete("/tasks/{task_id}/delete", status_code=200)
async def delete_task(task_id: int):
    async with aiosqlite.connect(DATABASE_URL) as db:
        cursor = await db.execute(
            "SELECT celery_task, kpt_file, kad_file, cover_file FROM ngw_tasks WHERE id = ?",
            (task_id,),
        )
        task_files = await cursor.fetchone()

        if task_files:
            task_files_list = list(task_files)
            celery.control.revoke(task_files_list[0], terminate=True)

            files_for_delete = task_files_list[1:]
            files_for_delete.append(f'data/temp/task_{task_id}_files.zip')

            await delete_paths(*files_for_delete)
            await execute_db_operations(db, ("DELETE FROM ngw_tasks WHERE id = ?", (task_id,)))

    return {'message': 'Задача успешно удалена'}


@app.post("/run_tasks", status_code=200)
async def run_task(files: list[UploadFile] = File(...), name: str = Form(None)):
    """
    Принимает один или несколько файлов (GeoJSON или ZIP).
    Загруженные файлы отправляются на обработку.

    Args:
        files: Список файлов для обработки.
        name: Название группы задач.

    Returns:
        Статус об успешной загрузке и обработке файлов.
    """
    errors = []

    try:
        moscow_time = datetime.now(ZoneInfo("Europe/Moscow"))

        db_group = TaskUploader.create_or_update(
            model=DBTasksGroup,
            params={'name': name or Path(files[0].filename).stem, 'added': moscow_time},
        )

        for file in files:
            paths = TaskUploader.upload_file(content=file, filename=file.filename)

            for path in paths:
                celery_uuid = uuid4()
                db_task = TaskUploader.create_or_update(
                    model=DBTask,
                    params={
                        'name': Path(path).stem,
                        'cover_file': path,
                        'added': moscow_time,
                        'group_id': db_group.id,
                        'celery_task': str(celery_uuid),
                    },
                )
                CollectKadTask().apply_async(args=(db_task.id,), task_id=str(celery_uuid))
    except Exception as e:
        errors.append(str(e))

    return {'message': 'Задачи успешно добавлены в очередь', 'errors': errors}


@app.post("/tasks/{task_id}/restart", status_code=200)
async def restart_task(task_id: int):
    """
    Перезапуск задачи по ее id.

    Args:
        task_id: id задачи.

    Returns:
        Статус об успешном перезапуске задачи.
    """
    db_task = TaskUploader.restart_task(task_id, celery)
    CollectKadTask().apply_async(args=(db_task.id,), task_id=db_task.celery_task)

    return {'message': 'Задача успешно перезапущена'}


# перезапуск группы задач
@app.post("/groups/{group_id}/restart", status_code=200)
async def restart_group(group_id: int):
    """
    Перезапуск группы задач по ее id.

    Args:
        group_id: id группы задач.

    Returns:
        Статус об успешном перезапуске группы задач.
    """

    async with aiosqlite.connect(DATABASE_URL) as db:
        cursor = await db.execute("SELECT id FROM ngw_tasks WHERE group_id = ?", (group_id,))
        tasks = await cursor.fetchall()

        moscow_time = datetime.now(ZoneInfo("Europe/Moscow"))

        TaskUploader.create_or_update(model=DBTasksGroup, instance=group_id, params={'added': moscow_time})

        for task in tasks:
            db_task = TaskUploader.restart_task(task[0], celery)
            CollectKadTask().apply_async(args=(db_task.id,), task_id=db_task.celery_task)

    return {'message': 'Группа успешно перезапущена'}

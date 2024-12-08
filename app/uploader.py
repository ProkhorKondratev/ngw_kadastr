from .db import DBTask, SessionLocal
from sqlalchemy import and_, or_, not_, case
from sqlalchemy.exc import SQLAlchemyError
from zoneinfo import ZoneInfo
from uuid import uuid4
from datetime import datetime
import os
import shutil
import zipfile
from pathlib import Path
import geopandas as gpd
import urllib3

urllib3.disable_warnings()


async def delete_paths(*paths: str):
    """
    Удаляет файлы и папки по указанным путям.
    """
    for path in paths:
        if os.path.exists(path):
            print(f"TaskUploader (delete): Удаление {path}")
            if os.path.isfile(path):
                os.remove(path)
            else:
                shutil.rmtree(path)


async def execute_db_operations(db, *queries):
    """
    Выполняет несколько операций с базой данных.
    """
    for query, params in queries:
        await db.execute(query, params)
    await db.commit()


async def create_archive(base_path: str, files: list, archive_name: str):
    """
    Создает архив с файлами.

    :param base_path: Путь, где будут временно сохранены файлы для архивации.
    :param files: Список путей к файлам для добавления в архив.
    :param archive_name: Полный путь к архиву, который будет создан.
    """
    os.makedirs(base_path, exist_ok=True)
    for file_path in files:
        if os.path.exists(file_path):
            shutil.copy(file_path, base_path)
    shutil.make_archive(
        base_name=archive_name,
        format='zip',
        root_dir='data/temp',
        base_dir=os.path.basename(base_path),
    )
    shutil.rmtree(base_path)


class TaskUploader:
    """
    Класс для загрузки файлов с задачами на обработку.
    """

    @staticmethod
    def find_path(file_path: str) -> str:
        """
        Получение пути к файлу.

        :param file_path: Путь к файлу.

        :return:
            Правильный путь к файлу.
        """
        if not file_path:
            raise Exception('TaskUploader (find_path): Не указан путь к файлу')

        base, extension = os.path.splitext(file_path)
        final_path = file_path

        counter = 1
        while os.path.exists(final_path):
            final_path = f"{base}({counter}){extension}"
            counter += 1

        return final_path

    @staticmethod
    def create_or_update(model, instance=None, params=None):
        """
        Создание или обновление экземпляра модели.

        :param model: Модель, для которой создается или обновляется экземпляр.
        :param instance: Экземпляр модели, который нужно обновить. Если не указан, создается новый экземпляр.
        :param params: Параметры для обновления экземпляра.

        :return:
            Экземпляр модели.
        """

        db = SessionLocal(expire_on_commit=False)
        try:
            if instance:
                instance_id = instance.id if isinstance(instance, model) else instance
                db_instance = db.query(model).filter(model.id == instance_id).first()
                if not db_instance:
                    raise ValueError(
                        f"TaskUploader (create_or_update): " f"{model.__tablename__} с ID {instance_id} не найден"
                    )

                if params:
                    for key, value in params.items():
                        setattr(db_instance, key, value)
            else:
                if params is None:
                    params = {}
                db_instance = model(**params)
                db.add(db_instance)

            db.commit()
            return db_instance
        except SQLAlchemyError as e:
            db.rollback()
            raise Exception(f"TaskUploader (create_or_update): Ошибка при создании или обновлении: {e}")
        finally:
            db.close()

    @staticmethod
    def get_working_tasks():
        """
        Получение всех задач, которые находятся в процессе выполнения.

        :return:
            Список необработанных задач.
        """

        db = SessionLocal(expire_on_commit=False)
        try:
            tasks = (
                db.query(DBTask)
                .filter(
                    not_(
                        and_(
                            DBTask.kpt_status["state"].as_string() == "SUCCESS",
                            DBTask.kad_status["state"].as_string() == "SUCCESS",
                        )
                    ),
                    not_(
                        or_(
                            DBTask.kpt_status["state"].as_string() == "FAILED",
                            DBTask.kad_status["state"].as_string() == "FAILED",
                        )
                    ),
                )
                .order_by(
                    case(
                        (
                            or_(
                                DBTask.kpt_status["state"].as_string() != "PREPARING",
                                DBTask.kad_status["state"].as_string() != "PREPARING",
                            ),
                            1,
                        ),
                        else_=2,
                    )
                )
            )

            return tasks

        except Exception as e:
            raise f"TaskUploader (get_working_tasks): Ошибка при получении задач: {e}"
        finally:
            db.close()

    @staticmethod
    def upload_file(content, filename=None, dest='data/uploaded/'):
        """
        Фабричный метод загрузки файла на обработку.
        Принимает zip и geojson файлы.
        возвращает список файлов для обработки.

        :param content: Файл для обработки. Может быть объектом файла или байтами.
        :param dest: Папка для сохранения файла.
        :param filename: Имя файла для сохранения. Если не указано, пытается использовать file.filename.
        :return:
            files: Список файлов для обработки.
        """

        if not filename:
            try:
                filename = content.filename
            except AttributeError:
                raise ValueError("TaskUploader (upload_file): Не указано имя файла для сохранения")

        try:
            content = content.file.read()
        except AttributeError:
            content = content

        file_factory = {
            '.zip': TaskUploader.process_zip,
            '.geojson': TaskUploader.process_file,
        }

        file_ext = Path(filename).suffix
        if file_ext not in file_factory:
            raise ValueError(f"TaskUploader (upload_file): Неизвестное расширение файла: {file_ext}")

        return file_factory[file_ext](content, dest, filename)

    @staticmethod
    def process_zip(content, dest, filename, parts=True):
        """
        Загрузка файла ZIP на обработку.
        Извлекает из архива все файлы с расширением .geojson и .shp и сохраняет их в папку uploaded.
        Файлы разбиваются на части и сохраняются в папку uploaded.

        :param content: Путь к zip файлу.
        :param dest: Папка для сохранения файла.
        :param filename: Имя файла для сохранения.
        :param parts: Разбивать файлы на части.

        :return:
            files: Список файлов для обработки.
        """

        zip_path = TaskUploader.find_path(dest + 'temp_' + filename)
        with open(zip_path, 'wb') as f:
            f.write(content)

        parts_files = []
        files = []

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for zip_info in zip_ref.infolist():
                if zip_info.is_dir():
                    continue

                file_ext = Path(zip_info.filename).suffix
                if not parts:
                    if file_ext in ['.geojson']:
                        base_name = Path(filename).stem + file_ext
                        final_path = TaskUploader.find_path(dest + base_name)
                        zip_info.filename = os.path.basename(final_path)
                        zip_ref.extract(zip_info, dest)
                        files.append(final_path)
                else:
                    if file_ext in ['.geojson', '.cpg', '.dbf', '.prj', '.shp', '.shx']:
                        base_name = 'temp_' + Path(filename).stem + file_ext
                        final_path = TaskUploader.find_path(dest + base_name)
                        zip_info.filename = os.path.basename(final_path)
                        zip_ref.extract(zip_info, dest)
                        if file_ext in ['.shp', '.geojson']:
                            parts_files.append(final_path)

        for part in parts_files:
            files.extend(TaskUploader.make_parts(dest, part, filename))

        TaskUploader.clean_files(dest)
        return files

    @staticmethod
    def process_file(content, dest, filename, parts=True):
        """
        Загрузка файла GeoJSON на обработку.
        Разбивает файл на части и сохраняет их в папку uploaded.

        :param content: Путь к geojson файлу.
        :param dest: Папка для сохранения файла.
        :param filename: Имя файла для сохранения.
        :param parts: Разбивать файлы на части.

        :return:
            files: Список файлов для обработки.
        """

        base_filename = "temp_" + filename if parts else filename
        geo_path = TaskUploader.find_path(dest + base_filename)

        with open(geo_path, 'wb') as f:
            f.write(content)

        files = TaskUploader.make_parts(dest, geo_path, filename) if parts else [geo_path]
        TaskUploader.clean_files(dest)
        return files

    @staticmethod
    def make_parts(dest, filepath, filename):
        gdf = gpd.read_file(filepath)

        if gdf.crs != 'EPSG:4326':
            gdf = gdf.to_crs('EPSG:4326')

        files = []

        for index, row in gdf.iterrows():
            base_name_parts = []

            if 'name' in row:
                base_name_parts.append(row['name'])

            if 'lpu' in row:
                base_name_parts.append(row['lpu'])

            if not base_name_parts:
                base_name_parts.extend([Path(filename).stem, str(index)])

            geo_name = dest + "_".join(base_name_parts) + '.geojson'
            geo_path = TaskUploader.find_path(geo_name)
            polygon = gdf[gdf.index == index]
            polygon.to_file(geo_path, driver='GeoJSON')
            files.append(geo_path)

        return files

    @staticmethod
    def clean_files(dest):
        for trash in os.listdir(dest):
            if trash.startswith('temp_'):
                os.remove(os.path.join(dest, trash))

    @staticmethod
    def restart_task(db_task, celery):
        """
        Перезапуск задачи.
        """
        db_task = TaskUploader.create_or_update(
            model=DBTask,
            instance=db_task,
        )

        files_for_delete = [
            db_task.kpt_file,
            db_task.kad_file,
            f'data/temp/task_{db_task.id}_files.zip',
        ]

        delete_paths(*files_for_delete)

        celery.control.revoke(db_task.celery_task, terminate=True)
        celery_uuid = uuid4()
        moscow_time = datetime.now(ZoneInfo("Europe/Moscow"))

        db_task = TaskUploader.create_or_update(
            model=DBTask,
            instance=db_task,
            params={
                'kpt_status': {'state': 'PREPARING'},
                'kad_status': {'state': 'PREPARING'},
                'kpt_file': None,
                'kad_file': None,
                'kpt_task_id': None,
                'kad_task_id': None,
                'added': moscow_time,
                'celery_task': str(celery_uuid),
            },
        )

        return db_task

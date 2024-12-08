import os
import requests
from requests.exceptions import RequestException, Timeout


class NGToolbox:
    upload_url = os.getenv('NGT_UPLOAD_URL')
    execute_url = os.getenv('NGT_EXECUTE_URL')
    status_url = os.getenv('NGT_STATUS_URL')
    token = os.getenv('NGT_TOKEN')
    api_key = os.getenv('NGT_API_KEY')
    headers = {'Authorization': 'Token ' + token}

    @staticmethod
    def make_request(url, req_type='get', data=None, json=None, params=None, timeout=30, max_attempts=5):
        attempt = 0
        while attempt < max_attempts:
            try:
                response = requests.request(
                    req_type,
                    url,
                    data=data,
                    params=params,
                    json=json,
                    headers=NGToolbox.headers,
                    verify=False,
                    timeout=timeout
                )

                response.raise_for_status()
                return response
            except Timeout:
                print(f"Попытка {attempt + 1} из {max_attempts}. Время ожидания ответа истекло.")
                attempt += 1
            except RequestException as e:
                raise Exception(f'TaskUploader (make_request): Ошибка при выполнении запроса к серверу:<br>{e}')

        raise Exception(f'TaskUploader (make_request): Превышено количество запросов к серверу ({max_attempts})')

    @staticmethod
    def upload(upload_file):
        try:
            with open(upload_file, 'rb') as f:
                url = NGToolbox.upload_url + os.path.basename(upload_file)
                response = NGToolbox.make_request(url, req_type='post', data=f)
                return response.text  # id файла на сервере
        except Exception as e:
            raise Exception('NGToolbox (upload): Ошибка при загрузке файла на сервер:<br>', e)

    @staticmethod
    def collect_kpt(file_id, mode='kpt', debug=False):
        if not file_id:
            raise Exception('NGToolbox (collect_kpt): Не указан ID файла для получения списка кварталов')

        json_request = {'operation': 'egrn_kvartals_cover', 'inputs': {}}
        json_request['inputs']['input_file'] = file_id
        json_request['inputs']['mode'] = mode
        json_request['inputs']['debug'] = debug

        response = NGToolbox.make_request(NGToolbox.execute_url, req_type='post', json=json_request)
        task_id = response.json()['task_id']
        return task_id  # id задачи на сервере

    @staticmethod
    def collect_kad(file_id):
        if not file_id:
            raise Exception('NGToolbox (collect_kad): Не указан ID файла для получения геометрии по кад. номерам')

        json_request = {'operation': 'cadnums_to_geodata', 'inputs': {}}
        json_request['inputs']['api_key'] = NGToolbox.api_key
        json_request['inputs']['source_file'] = file_id

        response = NGToolbox.make_request(NGToolbox.execute_url, req_type='post', json=json_request)
        task_id = response.json()['task_id']
        return task_id

    @staticmethod
    def status(task_id):
        if not task_id:
            raise Exception('NGToolbox (status): Не указан ID задачи для получения статуса')

        response = NGToolbox.make_request(NGToolbox.status_url + task_id + '/')
        response_data = response.json()
        response_data['task_id'] = task_id
        return response_data

    @staticmethod
    def download(file_url):
        if not file_url:
            raise Exception('NGToolbox (download): Не указан URL файла для скачивания')

        response = NGToolbox.make_request(file_url)
        return response.content

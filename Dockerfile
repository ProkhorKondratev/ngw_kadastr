FROM python:3.12.2-slim-bookworm

WORKDIR /usr/src/app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y redis-server supervisor curl

# Устанавливаем Poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry python && \
    cd /usr/local/bin && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false

# Устанавливаем зависимости Python
COPY ./pyproject.toml ./poetry.lock* ./
RUN poetry install --no-root

ENV PYTHONPATH=/usr/src/app

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY supervisord.conf /etc/supervisord.conf

COPY ./app ./app

# Запускаем supervisor
CMD ["/usr/bin/supervisord"]

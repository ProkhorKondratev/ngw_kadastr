#!/bin/bash

# Параметры подключения к удалённому серверу
REMOTE_USER=""
REMOTE_HOST=""
SCRIPT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REMOTE_PATH=""
INSTALL_SCRIPT="install.sh"

function handle_error {
    echo "Ошибка: $1"
    exit 1
}

echo "Остановка контейнеров на удалённом сервере..."
ssh $REMOTE_USER@$REMOTE_HOST "cd $REMOTE_PATH/share && sudo docker compose down" || handle_error "Не удалось остановить контейнеры на удалённом сервере."

echo "Копирование файлов на удалённый сервер..."
scp -r "$SCRIPT_PATH/share" $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH || handle_error "Не удалось скопировать файлы на удалённый сервер."

echo "Установка и запуск контейнеров на удалённом сервере..."
ssh $REMOTE_USER@$REMOTE_HOST "cd $REMOTE_PATH/share && ./$INSTALL_SCRIPT" || handle_error "Не удалось установить и запустить контейнеры на удалённом сервере."

echo "---------------------------------"
echo "Установка завершена."
exit 0

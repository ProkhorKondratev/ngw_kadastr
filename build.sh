#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

echo "Рабочая директория: $DIR"

echo "Сборка проекта.."
docker build -t ngw_kad:1.0.0 .


echo "Сборка завершена."
echo "---------------------------------"

echo "Сохранение образа.."
docker save ngw_kad:1.0.0 | gzip > ./share/ngw_kad.tar.gz

docker image rm -f ngw_kad:1.0.0
echo "Сохранение завершено."

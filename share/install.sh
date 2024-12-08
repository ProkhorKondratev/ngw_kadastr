#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

sudo docker image rm -f ngw_kad:1.0.0
sudo docker load < ngw_kad.tar.gz
sudo docker compose up -d

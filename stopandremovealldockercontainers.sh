#!/bin/bash

docker stop $(docker ps -a -q)
docker rm  $(docker ps -a -q)

docker system prune -a -f
docker volume prune -f
docker container prune -f
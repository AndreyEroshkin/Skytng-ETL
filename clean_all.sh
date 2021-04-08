#!/bin/sh
docker-compose down 
docker rm -f $(docker ps -a -q) 
docker volume prune

rm -rf ./logs
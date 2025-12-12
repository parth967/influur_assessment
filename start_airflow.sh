#!/bin/bash

mkdir -p logs dags plugins config output data

docker-compose down

docker-compose up -d

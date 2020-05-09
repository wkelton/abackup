#!/bin/bash

docker-compose up --no-start test_postgres
docker cp test_data/init_postgres.sql test-postgres:/docker-entrypoint-initdb.d
docker-compose up -d test_postgres

sleep 3

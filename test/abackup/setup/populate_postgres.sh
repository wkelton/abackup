#!/bin/bash

cat test_data/create_table.sql | docker exec -i test-postgres psql -U postgres -d bardb

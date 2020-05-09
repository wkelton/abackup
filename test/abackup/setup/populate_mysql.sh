#!/bin/bash

cat test_data/create_table.sql | docker exec -i test-mysql mysql -pfoobar testdb

#!/bin/bash


create_cmd="create table foo ( bar varchar(255) );"
insert_cmd="insert into foo (bar) values ('foobar');"

docker exec test-container mysql -pfoobar testdb --execute="$create_cmd"
docker exec test-container mysql -pfoobar testdb --execute="$insert_cmd"

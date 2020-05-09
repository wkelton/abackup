#!/bin/bash

result=$(docker exec -i test-mysql mysql -pfoobar testdb -e "$(cat test_data/verify_table.sql)")
error_code=$?

if [ $error_code -ne 0 ]; then
  echo "ERROR occurred when attempting to run SQL"
  exit $error_code
fi

result_code=$(echo "${result}" | tail -1 | grep -oE '\w+$')

if [[ "${result_code}" == "1" ]]; then
  echo "PASS"
  exit 0
elif [[ "${result_code}" == "0" ]]; then
  echo "FAIL"
  exit 1
else
  echo "UNKNOWN output: ${result_code} from ${result}"
  exit 2
fi

#!/bin/bash

set -e

docker cp test-single-tar:/data/bar/test_dir_baz.txt test_data/test_dir_baz_from_container.txt
diff test_data/test_dir_baz_from_container.txt test_data/test_dir_baz.txt
rm test_data/test_dir_baz_from_container.txt
echo PASS
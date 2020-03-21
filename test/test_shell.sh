#!/bin/bash

test_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 ; pwd -P)"
root_dir=$(dirname "${test_dir}")

bash --rcfile <( echo '
source ~/.bashrc

export PATH="'${root_dir}'":$PATH
export PYTHONPATH="'${root_dir}'/lib":$PYTHONPATH
')

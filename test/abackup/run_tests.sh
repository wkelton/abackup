#!/bin/bash

setup_dir=$(pwd)/setup

function die() {
  msg=$1
  err=$2

  echo "////////////////////////////"
  echo "        ERROR"
  echo "${msg}"
  echo
  echo "See the log:   absync --config backup.yml log"
  echo
  echo "Be sure to cleanup."

  exit ${err}
}

function finalize() {
  err=$1

  "${setup_dir}/tear_down.sh"

  echo
  echo "___________________________________________"

  if [ ${err} -ne 0 ]; then
    echo "                 FAIL"
  else
    echo "                 PASS"
  fi

  echo
  echo "***CLEANUP***: Be sure to delete cron settings"
  echo

  exit ${err}
}

echo
echo "Setting up Mysql container..."
echo
"${setup_dir}/init_mysql.sh"
"${setup_dir}/populate_mysql.sh"
"${setup_dir}/verify_mysql.sh" || die 'Verify failed after populating mysql.' $?
echo
echo "Setting up Postgres container..."
echo
"${setup_dir}/init_postgres.sh"
"${setup_dir}/populate_postgres.sh"
"${setup_dir}/verify_postgres.sh" || die 'Verify failed after populating postgres.' $?

abackup --config backup.yml examine


echo "################################################"
echo "    update-cron"
echo
abackup --config backup.yml update-cron
abackup --config backup.yml examine
echo


echo "################################################"
echo "    backup"
echo
abackup --config backup.yml backup --healthchecks || die 'Backup failed!' $?
abackup --config backup.yml examine
echo


echo "################################################"
echo "    restore"
echo

"${setup_dir}/tear_down.sh"
"${setup_dir}/init_mysql.sh"
"${setup_dir}/init_postgres.sh"

abackup --config backup.yml restore || die 'Restore failed!' $?

echo
echo "Verifying Mysql:"
"${setup_dir}/verify_mysql.sh"
mysql_code=$?
echo
echo "Verifying Postgres:"
"${setup_dir}/verify_postgres.sh"
postgres_code=$?
echo

abackup --config backup.yml log

if [[ $mysql_code -ne 0 || $postgres_code -ne 0 ]]; then
  finalize 1
else
  finalize 0
fi

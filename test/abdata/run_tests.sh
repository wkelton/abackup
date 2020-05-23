#!/bin/bash

function die() {
  msg=$1
  err=$2

  echo "////////////////////////////"
  echo "        ERROR"
  echo "${msg}"
  echo
  echo "See the log:   abdata --config data.yml log"
  echo
  echo "Be sure to cleanup."

  exit ${err}
}

function finalize() {
  err=$1

  echo
  echo "___________________________________________"

  echo
  echo "***CLEANUP***: Be sure to delete cron settings"
  echo

  exit ${err}
}


echo "################################################"
echo "    update-cron"
echo
abdata --config data.yml update-cron
echo


echo "################################################"
echo "    check"
echo
abdata --config data.yml check --healthchecks || die 'Check failed!' $?
echo


abdata --config data.yml log

finalize 0


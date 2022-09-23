#!/bin/bash

function die() {
  msg=$1
  err=$2

  echo "////////////////////////////"
  echo "        ERROR"
  echo "${msg}"
  echo
  echo "See the log:   absync --config sync.yml log"
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
absync --config sync.yml update-cron
absync --config sync.yml examine
echo


echo "################################################"
echo "    stored-path"
echo
absync --debug --config sync.yml stored-path stored1
echo


echo "################################################"
echo "    auto"
echo
absync --debug --config sync.yml auto --healthchecks || die 'Check failed!' $?
absync --config sync.yml examine
echo


absync --config sync.yml log

finalize 0


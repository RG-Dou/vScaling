#!/usr/bin/env bash

RESULT="$(dirname $(dirname $(pwd)))/tools/results/effect"

COUNT=5
while [ $COUNT -gt 0 ] ;
do
  echo $COUNT
  let COUNT=COUNT-1
  #msg=`python python/check_error.py $RESULT/$1/both`
  msg='error'
  echo $msg
  if [ $msg == 'error' ]
  then
#    bash run-module.sh 1 'both' $1
#    python -c 'import time; time.sleep(10)'
    echo 'try again'
  else
    echo 'successful'
  #  break
  fi
done

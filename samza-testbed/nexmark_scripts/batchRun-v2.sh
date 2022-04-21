#!/usr/bin/env bash

APPS="1 2 3 5 8 11"
MEMS="1150 1200 1250 1300"

for APP in $APPS; do
  for MEM in $MEMS; do
    for i in {1..5}; do
      ./run-final.sh $APP true true true 10000 $MEM
      ./run-final.sh $APP true false true 10000 $MEM
    done
  done
done


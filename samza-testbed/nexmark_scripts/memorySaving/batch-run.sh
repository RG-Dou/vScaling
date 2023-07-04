#!/usr/bin/env bash

APPS="1"
CORES="2 4 6 8"

for APP in $APPS; do
  for CORE in $CORES; do
      ./single-run.sh $APP 10000 $CORE
  done
done


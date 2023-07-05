#!/usr/bin/env bash

function cores(){
  APPS="1 2"
  CORES="2 4 6 8"

  for APP in $APPS; do
    for CORE in $CORES; do
        bash single-run.sh $APP 10000 $CORE "default"
        bash single-run.sh $APP 10000 $CORE "memorySaving"
    done
  done
}

function states() {
  APPS="3 8"
  STATES="500 2000 8000 16000"

  for APP in $APPS; do
    for STATE in $STATES; do
        bash single-run-state.sh $APP 10000 $STATE
    done
  done
}

states

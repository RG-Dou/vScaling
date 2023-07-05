#!/usr/bin/env bash

function cores(){
  APPS="1 2"
  CORES="2 4 6 8"

  for APP in $APPS; do
    for CORE in $CORES; do
        bash single-run.sh $APP 10000 $CORE "default" 26000
        bash single-run.sh $APP 10000 $CORE "memorySaving" 800
    done
  done
}

function states() {
  APPS="3 8"
  STATES="500 2000 8000 16000"

  for APP in $APPS; do
    for STATE in $STATES; do
        bash single-run-state.sh $APP 10 $STATE "default" 26000
        bash single-run-state.sh $APP 10 $STATE "memorySaving" 1500
    done
  done
}

cores
states

#!/usr/bin/env bash

##   Q1 : 10000
##   Q2 : 10000
##   Q3 : 10000
##   Q5 :
BASES="10000"
APP=11

for base in $BASES; do
  #./run-final.sh ${APP} true true true $base
  
  #Elasticutor
  #./run-final.sh ${APP} true false false $base
 
  #currrent arrival rate
 # ./run-final.sh ${APP} true true false $base

  #mem scheduling
  #./run-final.sh ${APP} false true true $base

  #cpu scheduling
  ./run-final.sh ${APP} true false true $base

  #no scheduling
  # ./run-final.sh ${APP} false false false $base
  #./run-final.sh ${APP} true true true $base
done

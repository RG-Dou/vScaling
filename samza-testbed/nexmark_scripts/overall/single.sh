#!/usr/bin/env bash

##   Q1 : 10000
##   Q2 : 10000
##   Q3 : 10000
##   Q5 :
BASES="10000"
APP=11

for base in $BASES; do
  #./base.sh ${APP} true true true $base
  
  #Elasticutor
  #./base.sh ${APP} true false false $base
 
  #currrent arrival rate
 # ./base.sh ${APP} true true false $base

  #mem scheduling
  #./base.sh ${APP} false true true $base

  #cpu scheduling
  ./base.sh ${APP} true false true $base

  #no scheduling
  # ./base.sh ${APP} false false false $base
  #./base.sh ${APP} true true true $base
done

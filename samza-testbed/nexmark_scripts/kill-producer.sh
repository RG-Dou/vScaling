#!/usr/bin bash

kill -9 $(jps | grep Generator | awk '{print $1}')


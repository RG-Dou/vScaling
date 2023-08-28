# For running config
IS_COMPILE=1
HOST="localhost"
DURATION=900

# For source config
CYCLE=300
BASE=300
RATE=320

# For policy config
Policy="default"
CPU_SWITCH="false"
MEM_SWITCH="false"
ARRIVAL_SWITCH="false"

# For resource config
CPU=2
NUM_EXECUTORS=1

# For store config
PARENT_DIR="GCInclude/"

function run_base() {
#    bash base.sh $IS_COMPILE $HOST $APP $DURATION $CYCLE $BASE $RATE $Policy $CPU_SWITCH $MEM_SWITCH $ARRIVAL_SWITCH $MEM $CPU $NUM_EXECUTORS $PARENT_DIR $CHILD_DIR
    bash base.sh $IS_COMPILE $HOST $1 $DURATION $CYCLE $BASE $RATE $Policy $CPU_SWITCH $MEM_SWITCH $ARRIVAL_SWITCH $3 $2 $NUM_EXECUTORS $PARENT_DIR$2 $4
    python -c 'import time; time.sleep(10)'
}

APPS="2 5 8"
MEMS="1000 3000 5000 7000 9000 11000 13000 15000"
CPUS="4 6"

for APP in $APPS; do
  for CPU in $CPUS; do
    for MEM in $MEMS; do
        run_base $APP $CPU $MEM $APP+$MEM
    done
  done
done
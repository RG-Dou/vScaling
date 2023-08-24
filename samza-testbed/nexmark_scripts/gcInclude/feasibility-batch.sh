# For running config
IS_COMPILE=1
HOST="dragon"
APP=1
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
PARENT_DIR="GCInclude"

function run_base() {
#    bash base.sh $IS_COMPILE $HOST $APP $DURATION $CYCLE $BASE $RATE $Policy $CPU_SWITCH $MEM_SWITCH $ARRIVAL_SWITCH $MEM $CPU $NUM_EXECUTORS $PARENT_DIR $CHILD_DIR
    bash base.sh $IS_COMPILE $HOST $1 $DURATION $CYCLE $BASE $RATE $Policy $CPU_SWITCH $MEM_SWITCH $ARRIVAL_SWITCH $2 $CPU $NUM_EXECUTORS $PARENT_DIR $3
    python -c 'import time; time.sleep(10)'
}

APPS=(1 2)
MEMS=(1000 1500 2000 2500 3000 3500)

for APP in $APPS; do
  for MEM in $MEMS; do
      run_base $APP $MEM $APP+$MEM
  done
done
RESULT="$(dirname $(dirname $(pwd)))/tools/results/effect"

#rm -rf $RESULT/$1
mkdir $RESULT

bash run-module.sh 1 'both'
python -c 'import time; time.sleep(10)'
while true
do
  msg=`python python/check_error.py $RESULT/both`
  if [ $msg == 'error' ]
  then
    bash run-module.sh 1 'both'
    python -c 'import time; time.sleep(10)'
  else
    break
  fi
done


bash run-module.sh 1 'memory'
python -c 'import time; time.sleep(10)'

COUNT=5
while [ $COUNT -gt 0 ] ;
do
  msg=`python python/check_error.py $RESULT/memory`
  if [ $msg == 'error' ]
  then
    bash run-module.sh 1 'memory'
    python -c 'import time; time.sleep(10)'
    let COUNT=COUNT-1
  else
    break
  fi
done

bash run-module.sh 1 'CPU'
python -c 'import time; time.sleep(10)'

bash run-module.sh 1 'current'
python -c 'import time; time.sleep(10)'

bash run-module.sh 1 'static'

cd python
python draw_effect.py $RESULT

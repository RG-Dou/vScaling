RESULT="$(dirname $(dirname $(pwd)))/tools/results/effect"

rm -rf $RESULT/$1
mkdir $RESULT/$1

#bash run-module.sh 1 'memory' $1
#python -c 'import time; time.sleep(10)'

bash run-module.sh 1 'both' $1
python -c 'import time; time.sleep(10)'

#bash run-module.sh 1 'CPU' $1
#python -c 'import time; time.sleep(10)'

#bash run-module.sh 1 'current' $1
#python -c 'import time; time.sleep(10)'

#bash run-module.sh 1 'static' $1

cd python
python draw_effect.py $RESULT/$1

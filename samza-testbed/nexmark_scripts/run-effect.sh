
#bash run-module.sh 1 'Both Scheduling'
#bash run-module.sh 1 'CPU Scheduling'
#bash run-module.sh 1 'Memory Scheduling'
#bash run-module.sh 1 'Both with Current Arrival Rate'
bash run-module.sh 1 'Static'
#bash draw_effect.sh 1
RESULT="$(dirname $(dirname $(pwd)))/tools/results/effect"
cd python
python draw_effect.py $RESULT

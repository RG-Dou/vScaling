CUR_DIR=pwd
ROOT="$(dirname $(dirname $(pwd)))"
RESULT=$ROOT/tools/results
echo $RESULT
cd $RESULT || exit
rm -rf $1
mkdir -rf $1

cd $CUR_DIR || exit
bash run-app.sh $1 default
# bash run-app.sh $1 elasticutor
cd python || exit
python draw_comp.py $RESULT/$1

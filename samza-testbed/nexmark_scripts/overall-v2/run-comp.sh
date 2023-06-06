CUR_DIR=`pwd`
ROOT="$(dirname $(dirname $(dirname $(pwd))))"
RESULT=$ROOT/tools/results
mkdir $RESULT
echo $RESULT
cd $RESULT || exit
rm -rf $1
mkdir $1

cd $CUR_DIR || exit
bash run-app.sh $1 default
bash run-app.sh $1 elasticutor
cd python || exit
python ../python/draw_comp.py $RESULT/$1

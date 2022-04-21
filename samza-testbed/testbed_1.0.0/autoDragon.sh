#for each expeirment setting
#/home/samza/clearKafkaAndZooKeeper.sh
while IFS=" " read -r nOE L T l_low l_high deltaT mInterval dFactor muFactor
do
    echo $nOE $L $T $l_low $l_high $deltaT $mInterval $dFactor $muFactor;
    #Modify applicaiton config
    cp ~/projects/work2/samza-benchmark/samza-testbed/testbed_1.0.0/src/main/config/stock-exchange-ss-dragon-backlogdelay.properties properties.t1
    awk -F"=" 'BEGIN{OFS=FS} $1=="job.container.count"{$2='"$nOE"'}1' properties.t1 > properties.t2
#    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.requirement.window"{$2='"$((T*1000))"'}1' properties.t2 > properties.t1
#    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.system.l_low"{$2='"$((l_low))"'}1' properties.t1 > properties.t2
#    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.system.l_high"{$2='"$((l_high))"'}1' properties.t2 > properties.t1
#    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.requirement.latency"{$2='"$((L))"'}1' properties.t1 > properties.t2
#    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.system.metrics_interval"{$2='"$((deltaT))"'}1' properties.t2 > properties.t1
#    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.system.migration_interval"{$2='"$((mInterval))"'}1' properties.t1 > properties.t2
#    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.system.decayfactor"{$2='"$dFactor"'}1' properties.t2 > properties.t1
#    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.system.conservative"{$2=sprintf("%s", '$muFactor')  }1' properties.t1 > properties.t2
    rm properties.t1
    mv properties.t2 ~/projects/work2/samza-benchmark/samza-testbed/testbed_1.0.0/src/main/config/stock-exchange-ss-dragon-backlogdelay.properties
    #Get appid
    date
    echo 'Start application...'
#    OUTPUT=`./submit-ss-dragon-autostop.sh 1 1 1 1`
    ./submit-ss-dragon-autostop.sh 1 1 1 1
    #echo $OUTPUT
    app=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
    appid=${app#application_}
    #appid='12345'
    #echo '!!!appid!!! '
#    echo $appid
    #Copy logs
#    echo 'Start downloading logs...'
#    localDir="4h_${nOE}_L${L}T${T}l${l_low}h${l_high}d${deltaT}m${mInterval}_newapp_conservative${muFactor}_delay5000us_serializeoptimized_multisourcestargets_backlogTriggerAlgorithm_dragon"
#    app="application_1586407609192_0001"
#    app="application_${appid}"
#    echo ${nOE}
#    echo ${appid}
#    python /home/samza/copyLogs.py ${app} ${localDir}
    
    # Copy eagle's log
#    ssh eagle "
#        python /home/samza/copyLogs.py ${app} ${localDir} 
#    "
    
    
    #echo 'what'
    #localDir="/data/samza/samza_result/4h_${nOE}_L${L}T${T}l${l}d${deltaT}"
#    echo $locaDir
#    ./runCprhost.sh dragon ${appid} ${localDir} 
done < ~/tools/paramlist

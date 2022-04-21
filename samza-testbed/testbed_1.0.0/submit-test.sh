OUTPUT="$(./target/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/src/main/config/stock-exchange-ss-camel.properties | grep 'application_.*$')"

#OUTPUT="$(ls | grep '^su.*$')"
appid=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`

echo "${appid}"
#
#OUT="$(date)"
#echo "${OUT}"
#sleep 5
#echo "${OUT}"


#!/usr/bin/env bash

cd hello-samza

./bin/grid start zookeeper
python -c 'import time; time.sleep(5)'

./bin/grid start kafka
python -c 'import time; time.sleep(5)'

./deploy/samza/bin/run-class.sh samza.examples.wikipedia.application.WikipediaZkLocalApplication  \
  --config job.config.loader.factory=org.apache.samza.config.loaders.PropertiesConfigLoaderFactory \
  --config job.config.loader.properties.path=$PWD/deploy/samza/config/wikipedia-application-local-runner.properties

FROM ubuntu:14.04

RUN apt-get update

RUN apt-get install -y default-jdk

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN apt-get install -y git

RUN apt-get install -y maven

RUN git clone https://github.com/RG-Dou/vScaling.git

RUN apt-get install -y curl

RUN cd vScaling && \
  git checkout k8s-version

RUN cd vScaling/hello-samza && \
  ls -a

RUN cd vScaling/hello-samza && \
  mvn clean package

RUN cd vScaling/hello-samza && \
  mkdir -p deploy/samza

RUN cd vScaling/hello-samza && \
  tar -xvf ./target/hello-samza-1.1.0-dist.tar.gz -C deploy/samza

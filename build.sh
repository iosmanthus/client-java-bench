#!/usr/bin/env bash

mvn clean package -Dmaven.test.skip=true
docker build -t hub.pingcap.net/keyspace/client-java-bench:v0.1.0 .
docker push hub.pingcap.net/keyspace/client-java-bench:v0.1.0

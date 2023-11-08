#!/usr/bin/env bash

mvn clean package -Dmaven.test.skip=true
docker build -t hub.pingcap.net/keyspace/client-java-bench:latest .
docker push hub.pingcap.net/keyspace/client-java-bench:latest

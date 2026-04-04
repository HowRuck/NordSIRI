#!/bin/bash

java -Dserver.port=8888 -Dspring.aot.enabled=true -jar target/SiriAnalyzer-0.0.1-SNAPSHOT.jar

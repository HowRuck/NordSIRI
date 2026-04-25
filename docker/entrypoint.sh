#!/bin/sh
set -eu

exec java \
  -XX:+UseG1GC \
  -XX:+UseStringDeduplication \
  -XX:+UnlockExperimentalVMOptions \
  -XX:+UseCompactObjectHeaders \
  -XX:G1PeriodicGCInterval=30000 \
  -XX:MinHeapFreeRatio=10 \
  -XX:MaxHeapFreeRatio=20 \
  -Xss512k \
  -XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -XX:+ExitOnOutOfMemoryError \
  -Dserver.port=8888 \
  -Dspring.aot.enabled=true \
  -Dhibernate.bytecode.use_reflection_optimizer=true \
  -jar /app/app.jar

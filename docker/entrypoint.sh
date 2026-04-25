#!/bin/bash
set -euo pipefail

JAVA_OPTS="\
  -XX:+UseG1GC \
  -XX:+UseStringDeduplication \
  -XX:+UnlockExperimentalVMOptions \
  -XX:UseCompactObjectHeaders=true \
  -XX:+UseLaydenOptimizations \
  # Returns RAM to OS quickly when idle
  -XX:G1PeriodicGCInterval=30000 \
  -XX:MinHeapFreeRatio=10 \
  -XX:MaxHeapFreeRatio=20 \
  # Reduces thread stack size
  -Xss512k \
  # Container awareness
  -XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -XX:+ExitOnOutOfMemoryError \
  # Spring/Hibernate tweaks for memory
  -Dserver.port=8888 \
  -Dspring.aot.enabled=true \
  -Dspring.main.lazy-initialization=true \
  -Dhibernate.bytecode.use_reflection_optimizer=true"

exec java ${JAVA_OPTS} -jar /app/app.jar

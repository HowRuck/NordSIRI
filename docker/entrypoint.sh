#!/bin/bash
set -euo pipefail

JAVA_OPTS="\
  -XX:+UseG1GC \
  -XX:+UseStringDeduplication \
  -XX:+ExitOnOutOfMemoryError \
  -XX:CRaCCheckpointTo=/crac \
  -XX:+UseContainerSupport \
  -Dserver.port=8888 \
  -Dspring.aot.enabled=true \
  -Dspring.backgroundpreinitializer.ignore=true \
  -Dhibernate.bytecode.use_reflection_optimizer=true \
  -Djdk.crac=true"

# If checkpoint exists, restore from it
if [ -f /crac/ckp_with_jvm.dat ]; then
  exec java ${JAVA_OPTS} -XX:+CRaCRestoreFrom=/crac -jar /app/app.jar
else
  # First run: create checkpoint after JVM warmup
  exec java ${JAVA_OPTS} -XX:+CRaCCheckpointOnExit -jar /app/app.jar
fi

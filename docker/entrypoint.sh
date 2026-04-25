#!/bin/bash
set -euo pipefail

exec java \
  -XX:+UseG1GC \
  -XX:+UseStringDeduplication \
  -XX:+ExitOnOutOfMemoryError \
  -Dserver.port=8888 \
  -Dspring.aot.enabled=true \
  -Dspring.backgroundpreinitializer.ignore=true \
  -Dhibernate.bytecode.use_reflection_optimizer=true \
  -jar /app/SiriAnalyzer.jar

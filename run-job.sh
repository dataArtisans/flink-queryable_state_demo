#!/usr/bin/env bash

base="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
target="$base/target/queryablestatedemo-1.0-SNAPSHOT.jar"

if [ ! -f $target ]; then
  echo "[info] Building demo JAR... this can take a few seconds."
  mvn clean package -DskipTests &> /dev/null
  echo "[info] Done. Demo JAR created in $target."
fi

echo "[info] Executing EventCountJob from queryablestatedemo-1.0-SNAPSHOT.jar (exit via Control+C)"
java -cp $target com.dataartisans.queryablestatedemo.EventCountJob

#!/bin/bash

# This script needs to be located where the build.sbt file is.
# It takes as optional parameter the path of the spark directory. If no path is specified
# it will look for the spark directory in $HOME.
#
# Example:  ./build_and_submit.sh MainQueryA
#
# Paramters:
#  - $1 : job to execute

set -o pipefail

echo -e "\n --- building .jar --- \n"

sbt assembly || { echo 'Build failed' ; exit 1; }

echo -e "\n --- spark-submit --- \n"

if [ -z "${SPARK_HOME+x}" ]; then sparkSubmit="spark-submit"; else sparkSubmit="$SPARK_HOME/bin/spark-submit"; fi


$sparkSubmit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp" --driver-memory 10g --class fr.telecom.$1 target/scala-2.11/*.jar "${@:2}"

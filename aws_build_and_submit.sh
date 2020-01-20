#!/bin/bash

set -o pipefail

echo -e "\n --- assemble .jar --- \n"

sbt assembly

echo -e "\n --- upload .jar --- \n"

aws s3 cp target/scala-2.11/GDELT-Explore-assembly-0.1.0.jar s3://fufu-program/jars/

echo -e "\n --- spark-submit through EMR step --- \n"

aws emr add-steps --cluster-id $1 --steps "file://$2"


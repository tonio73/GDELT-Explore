#!/bin/bash

sbt assembly

aws s3 cp target/scala-2.11/GDELT-Explore-assembly-0.1.0.jar s3://fufu-program/jars/

aws emr add-steps --cluster-id $1 --steps "file://$2"


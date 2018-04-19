#!/usr/bin/env bash

mode=$1
version=1.1.0-SNAPSHOT

usage() {
	echo -e "\nRate Structured Streaming Launcher"
	echo -e "\nUsage:"
	echo -e "CLIENT MODE  -> 'nohup ./run.sh client > client.out 2>&1&'"
	echo -e "CLUSTER MODE -> 'nohup ./run.sh cluster > cluster.out 2>&1&'\n"
	}

if [ "${mode}" = "cluster" ]; then
    echo "RUNNING cluster mode"
    spark2-submit \
      --class "bigdata.streaming.StructuredStreamingApp" \
      --master yarn \
      --files conf/application.conf \
      --deploy-mode cluster \
      lib/spark-structured-streaming-assembly-${version}.jar -c application.conf

elif [ "${mode}" = "client" ]; then
    echo "RUNNING client mode"
    spark2-submit \
      --class "bigdata.streaming.StructuredStreamingApp" \
      --master yarn \
      --files conf/application2.conf \
      --deploy-mode client \
      lib/spark-structured-streaming-assembly-${version}.jar -c conf/application2.conf

else
    usage
fi

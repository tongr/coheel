#!/bin/bash

set -e

RESULTS_FOLDER=${1:-hdfs://tenemhead2/home/stefan.bunk/results}

echo "This script is to be run after the training data program and before the"
echo "actual training."
echo "It downloads the training and test set files typed-training-data-3786-{12345,678910}.wiki"
echo "and typed-training-data-632-{12345,678910}.wiki and merges them."
echo ""
echo "Will work on the folder: ${RESULTS_FOLDER}. Abort in next five seconds if wrong."
sleep 5

echo "Downloading .."
$HADOOP_HOME/bin/hdfs dfs -getmerge $RESULTS_FOLDER/typed-training-data-3786-{12345,678910}.wiki ./typed-training-data-3786-2015-11.wiki
$HADOOP_HOME/bin/hdfs dfs -getmerge $RESULTS_FOLDER/typed-training-data-632-{12345,678910}.wiki ./typed-training-data-632-2015-11.wiki

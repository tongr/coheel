#!/bin/bash

set -e

RESULTS_FOLDER=${1:-hdfs://tenemhead2/home/stefan.bunk/results}
MASTER_ADDRESS=${2:-stefan.bunk@tenemhead2}
LOCAL_MASTER_FOLDER=${3:-/home/stefan.bunk/}

echo "This script is to be run after the surface link program and before the training data program."
echo "It downloads the surface links probs, concatenates them to two files 12345 and 678910."
echo "These files can then be uploaded to the tenems."
echo "We do this by uploading to tenemhead and then from there to the tenems, because its faster."
echo ""
echo "Will work on the folder: ${RESULTS_FOLDER}. Abort in next five seconds if wrong."
sleep 5

echo "Downloading .."
$HADOOP_HOME/bin/hdfs dfs -copyToLocal $RESULTS_FOLDER/surface-link-probs.wiki

cat surface-link-probs.wiki/12345/* > 12345
cat surface-link-probs.wiki/678910/* > 678910

scp bin/copy-to-slaves.sh $MASTER_ADDRESS:$LOCAL_MASTER_FOLDER
scp 12345 $MASTER_ADDRESS:$LOCAL_MASTER_FOLDER
scp 678910 $MASTER_ADDRESS:$LOCAL_MASTER_FOLDER

ssh $MASTER_ADDRESS "sudo -u hadoop10 bash ${LOCAL_MASTER_FOLDER}copy-to-slaves.sh ${LOCAL_MASTER_FOLDER}12345"
ssh $MASTER_ADDRESS "sudo -u hadoop10 bash ${LOCAL_MASTER_FOLDER}copy-to-slaves.sh ${LOCAL_MASTER_FOLDER}678910"

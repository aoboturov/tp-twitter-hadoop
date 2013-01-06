#!/bin/bash

if [ $# = 0 ]; then
  echo "expect month as a parameter [06| ... |12]"
  exit
fi

if [ -z "$TP_TWITTER_PROJECT_BASE_DIR" ]; then echo "One must set the TP_TWITTER_PROJECT_BASE_DIR variable"; exit; fi
if [ -z "$INPUT_DATA_DIR" ]; then echo "One must set the INPUT_DATA_DIR variable"; exit; fi
if [ -z "$OUTPUT_DATA_DIR" ]; then echo "One must set the OUTPUT_DATA_DIR variable"; exit; fi
if [ -z "$JOB_OWNER_EMAIL" ]; then echo "One must set the JOB_OWNER_EMAIL variable"; exit; fi

# Script defined variables
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx4096m"
export TP_TWITTER_JAR="${TP_TWITTER_PROJECT_BASE_DIR}/target/twitter-jobs-standalone-personal-server-jar-with-dependencies.jar"
export JOB_TO_RUN="com.oboturov.ht.stage2.KeywordsProcessing"
export JOB_CONFIG="${TP_TWITTER_PROJECT_BASE_DIR}/hadoop-configs/hadoop-personal-server-local.xml"
export INPUT_FILE_DIR="generated-nuplets-raw"
export INPUT_FILE_NAME="generated-nuplets-raw-2009-$1"
export OUTPUT_FILE_NAME="keywords-processed-nuplets-2009-$1"
export INPUT_FILES_LIST="`ls -m ${INPUT_DATA_DIR}/${INPUT_FILE_DIR}/${INPUT_FILE_NAME}/*.bz2`"
export INPUT_FILES_CONCATENATED="`echo $INPUT_FILES_LIST | tr -d ' '`"
export OUTPUT_DIR="${OUTPUT_DATA_DIR}/${OUTPUT_FILE_NAME}"
export LOG_FILE="${OUTPUT_FILE_NAME}.log"

rm $LOG_FILE
hadoop jar $TP_TWITTER_JAR $JOB_TO_RUN -conf $JOB_CONFIG $INPUT_FILES_CONCATENATED $OUTPUT_DIR >> $LOG_FILE 2>&1
tail -n 200 $LOG_FILE | mail -s "$JOB_TO_RUN $1 Hadoop job for the $INPUT_FILE_NAME had finished" $JOB_OWNER_EMAIL

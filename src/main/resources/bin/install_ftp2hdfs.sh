#!/bin/bash
binDir=`dirname "$0"`
echo $binDir

HADOOP_HOME_PATH=/usr/hdp/current/hadoop-client
HADOOP_CONFIG_SCRIPT=$HADOOP_HOME_PATH/libexec/hadoop-config.sh
HADOOP_CLIENT_LIBS=$HADOOP_HOME_PATH/client
if [ -e $HADOOP_CONFIG_SCRIPT ] ; then
        .  $HADOOP_CONFIG_SCRIPT
else
        echo "Hadoop Client not Installed on Node"
        exit 1
fi

FTP2HDFSJAR=`ls -1 $binDir/lib/ftp2hdfs*.jar`
COMMONSNETJAR=`ls -1 $binDir/lib/commons-net*.jar`

hadoop fs -mkdir -p /apps/copybook2tsv
hadoop fs -copyFromLocal -f $FTP2HDFSJAR /apps/ftp2hdfs/ftp2hdfs.jar
hadoop fs -copyFromLocal -f $COMMONSNETJAR /apps/ftp2hdfs/commons-net.jar
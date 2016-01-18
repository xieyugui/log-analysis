#!/usr/bin/env bash

# This file is sourced when running various Spark programs.
# Copy it as spark-env.sh and edit that to configure Spark for your site.

# Options read when launching programs locally with
# ./bin/run-example or ./bin/spark-submit
# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
# - SPARK_PUBLIC_DNS, to set the public dns name of the driver program
# - SPARK_CLASSPATH, default classpath entries to append

# Options read by executors and drivers running inside the cluster
# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
# - SPARK_PUBLIC_DNS, to set the public DNS name of the driver program
# - SPARK_CLASSPATH, default classpath entries to append
# - SPARK_LOCAL_DIRS, storage directories to use on this node for shuffle and RDD data
# - MESOS_NATIVE_JAVA_LIBRARY, to point to your libmesos.so if you use Mesos

# Options read in YARN client mode
# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
# - SPARK_EXECUTOR_INSTANCES, Number of workers to start (Default: 2)
#在Yarn集群中启动的Worker的数目
export SPARK_EXECUTOR_INSTANCES=4
# - SPARK_EXECUTOR_CORES, Number of cores for the workers (Default: 1).
#每个Worker所占用的CPU核的数目
export SPARK_EXECUTOR_CORES=3
# - SPARK_EXECUTOR_MEMORY, Memory per Worker (e.g. 1000M, 2G) (Default: 1G)
#每个Worker所占用的内存大小
export SPARK_EXECUTOR_MEMORY=6G
# - SPARK_DRIVER_MEMORY, Memory for Master (e.g. 1000M, 2G) (Default: 512 Mb)
#Spark应用程序Application所占的内存大小，这里的Driver对应Yarn中的ApplicationMaster
export SPARK_DRIVER_MEMORY=6G
# - SPARK_YARN_APP_NAME, The name of your application (Default: Spark)
export SPARK_YARN_APP_NAME=Spark-On-Yarn-Mc
# - SPARK_YARN_QUEUE, The hadoop queue to use for allocation requests (Default: ‘default’)
# - SPARK_YARN_DIST_FILES, Comma separated list of files to be distributed with the job.
# - SPARK_YARN_DIST_ARCHIVES, Comma separated list of archives to be distributed with the job.

# Options for the daemons used in the standalone deploy mode
# - SPARK_MASTER_IP, to bind the master to a different IP address or hostname
export SPARK_MASTER_IP=master
# - SPARK_MASTER_PORT / SPARK_MASTER_WEBUI_PORT, to use non-default ports for the master
export SPARK_MASTER_PORT=7077
# - SPARK_MASTER_OPTS, to set config properties only for the master (e.g. "-Dx=y")
# - SPARK_WORKER_CORES, to set the number of cores to use on this machine
export SPARK_WORKER_CORES=3
# - SPARK_WORKER_MEMORY, to set how much total memory workers have to give executors (e.g. 1000m, 2g)
export SPARK_WORKER_MEMORY=6G
# - SPARK_WORKER_PORT / SPARK_WORKER_WEBUI_PORT, to use non-default ports for the worker
# - SPARK_WORKER_INSTANCES, to set the number of worker processes per node
export SPARK_WORKER_INSTANCES=4
# - SPARK_WORKER_DIR, to set the working directory of worker processes
export SPARK_WORKER_DIR=/home/hadoop/logs
# - SPARK_WORKER_OPTS, to set config properties only for the worker (e.g. "-Dx=y")
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=600 -Dspark.worker.cleanupappDataTtl=600 -Dspark.cleaner.ttl=600"
# - SPARK_HISTORY_OPTS, to set config properties only for the history server (e.g. "-Dx=y")
# - SPARK_SHUFFLE_OPTS, to set config properties only for the external shuffle service (e.g. "-Dx=y")
# - SPARK_DAEMON_JAVA_OPTS, to set config properties for all daemons (e.g. "-Dx=y")
#export SPARK_DAEMON_JAVA_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/home/hadoop/logs -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+DisableExplicitGC -Xms1024m -Xmx2048m -XX:PermSize=128m -XX:MaxPermSize=256m"
# - SPARK_PUBLIC_DNS, to set the public dns name of the master or workers
#export HADOOP_CONF_DIR=/home/xie/soft/Spark/hadoop-2.6.0/etc/hadoop

export SPARK_LIBARY_PATH=.:$JAVA_HOME/lib:$JAVA_HOME/jre/lib:$HADOOP_HOME/lib/native:$SPARK_HOME/lib
export SPARK_SSH_OPTS="-p22 -o StrictHostKeyChecking=no"
export LD_LIBRARY_PATH=${HADOOP_HOME}/lib/lzo
# Generic options for the daemons used in the standalone deploy mode
# - SPARK_CONF_DIR      Alternate conf dir. (Default: ${SPARK_HOME}/conf)
# - SPARK_LOG_DIR       Where log files are stored.  (Default: ${SPARK_HOME}/logs)
# - SPARK_PID_DIR       Where the pid file is stored. (Default: /tmp)
# - SPARK_IDENT_STRING  A string representing this instance of spark. (Default: $USER)
# - SPARK_NICENESS      The scheduling priority for daemons. (Default: 0)
export SPARK_PID_DIR=/home/hadoop/pids

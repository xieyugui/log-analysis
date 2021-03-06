export HADOOP_HOME=/home/hadoop/hadoop
export SPARK_HOME=$HADOOP_HOME/spark
export HADOOP_YARN_HOME=$HADOOP_HOME
export SCALA_HOME=/home/hadoop/scala
export CASSANDRA_HOME=/home/hadoop/cassandra

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_PID_DIR=$HADOOP_HOME

export HADOOP_CLASSPATH=$HADOOP_HOME/lib
export SPARK_CLASSPATH=$SPARK_HOME/lib
export CASSANDRA_CLASSPATH=$CASSANDRA_HOME/lib

export JAVA_HOME=/home/hadoop/jdk
export JRE_HOME=$JAVA_HOME/jre
export CLASSPATH=.:$HADOOP_HOME/lib:$SPARK_HOME/lib:$JAVA_HOME/lib:$JRE_HOME/lib:$CLASSPATH:$CASSANDRA_HOME/lib
export PATH=$CASSANDRA_HOME/bin:$SCALA_HOME/bin:$SPARK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$JRE_HOME/bin:$PATH

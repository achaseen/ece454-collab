#!/bin/bash
if [ $# -ne 2 ]; then
 echo "Usage: ./compile_n_run javafile input_dir"
 exit
fi

JavaFile=$1
InputDir=$2

# clean up
hdfs dfs -rm -r -skipTrash /user/username/output
rm $JavaFile.jar
rm $JavaFile*.class

javac $JavaFile.java
jar cf $JavaFile.jar $JavaFile*.class
export HADOOP_CLASSPATH=$(pwd)/$JavaFile.jar
hadoop jar $JavaFile.jar $JavaFile /user/username/$InputDir /user/username/output

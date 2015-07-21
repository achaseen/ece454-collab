#!/bin/bash
if [ $# -ne 2 ]; then
 echo "Usage: ./compile_n_run javafile input_file"
 exit
fi

File=$1
InputDir=$2

# clean up
hdfs dfs -rm -r -skipTrash /user/achaseen/output
rm $File.jar
rm $File*.class

export CLASSPATH=$(hadoop classpath):/usr/hdp/current/pig-client/pig-0.14.0.2.2.4.2-2-core-h2.jar
scp achaseen@192.168.241.128:/home/achaseen/pig/$File\.java .
scp achaseen@192.168.241.128:/home/achaseen/pig/$File\.pig .
javac $File\.java
jar -cf $File.jar $File*.class
pig -param input=$InputDir -param output=output $File\.pig

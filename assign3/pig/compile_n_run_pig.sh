#!/bin/bash
if [ $# -ne 2 ]; then
 echo "Usage: ./compile_n_run javafile input_file"
 exit
fi

File=$1
InputDir=$2

# clean up
hdfs dfs -rm -r -skipTrash /user/username/output
rm $File.jar
rm $File*.class

scp daniel@192.168.1.19:/home/daniel/school/ece454/assign3/ece454-collab/assign3/pig/$File\.java .
scp daniel@192.168.1.19:/home/daniel/school/ece454/assign3/ece454-collab/assign3/pig/$File\.pig .
javac $File\.java
jar -cf $File.jar $File*.class
pig -param input=$inputDir -param output=output $File\.pig

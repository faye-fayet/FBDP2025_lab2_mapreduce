#!/bin/bash
export HADOOP_HOME=/home/faye/Desktop/bigDataProcess/hadoop_installs/hadoop-3.4.0
HADOOP_CP=$($HADOOP_HOME/bin/hadoop classpath)

rm -rf classes
mkdir -p classes

echo "正在编译..."
javac -cp "$HADOOP_CP" -d classes src/com/bigdata/task2/*.java

if [ $? -eq 0 ]; then
    echo "编译成功！"
    jar -cvf ../../jars/task2-merchant-distance.jar -C classes .
    echo "打包完成！JAR文件: ../../jars/task2-merchant-distance.jar"
else
    echo "编译失败！"
    exit 1
fi

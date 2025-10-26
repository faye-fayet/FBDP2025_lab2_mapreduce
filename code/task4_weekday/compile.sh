#!/bin/bash
export HADOOP_HOME=/home/faye/Desktop/bigDataProcess/hadoop_installs/hadoop-3.4.0
HADOOP_CP=$($HADOOP_HOME/bin/hadoop classpath)

rm -rf classes
mkdir -p classes

echo "正在编译周末工作日分析..."
javac -cp "$HADOOP_CP" -d classes src/com/bigdata/task4/weekday/*.java

if [ $? -eq 0 ]; then
    echo "编译成功！"
    jar -cvf ../../jars/task4-weekday-analysis.jar -C classes .
    echo "打包完成！"
else
    echo "编译失败！"
    exit 1
fi

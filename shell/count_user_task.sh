#!/bin/bash
DAY=`date -d "180 minutes ago" +'%Y-%m-%d'`
HOUR=`date -d "180 minutes ago" +'%H:%M:%S'`
#The JiaKuanSfTcpTask is for add partition and compress partition ,please keep JiaKuanSfHttpTask first


/home/qingyuan_ziyan_01/spark-hdp-1.6.3/bin/spark-submit   --principal qingyuan_ziyan_01@GDSAI.COM --keytab /home/qingyuan_ziyan_01/qingyuan_ziyan_01.keytab  --conf spark.local.dir=/hive_data/qingyuan_ziyan_01/logs --conf spark.driver.extrajavaoption="-XX:+UseG1GC" --master yarn --deploy-mode client --driver-memory 8G --executor-memory 4G --num-executors 30 --queue qingyuan_ziyan_01 --class com.ibstech.xdr.quarteranalysis.tasks.spring.JiaKuanCountUser --name SPARK_COUNT_USER /home/qingyuan_ziyan_01/qingyuan-15min-table-0.0.1.jar ${DAY} ${HOUR} 
/home/qingyuan_ziyan_01/spark-hdp-1.6.3/bin/spark-submit   --principal qingyuan_ziyan_01@GDSAI.COM --keytab /home/qingyuan_ziyan_01/qingyuan_ziyan_01.keytab  --conf spark.local.dir=/hive_data/qingyuan_ziyan_01/logs --conf spark.driver.extrajavaoption="-XX:+UseG1GC" --master yarn --deploy-mode client --driver-memory 4G --executor-memory 4G --num-executors 30 --queue qingyuan_ziyan_01 --class com.ibstech.xdr.quarteranalysis.tasks.spring.JiaKuanRadiusCountUser --name SPARK_RADIUS_COUNT_USER /home/qingyuan_ziyan_01/qingyuan-15min-table-0.0.1.jar ${DAY} ${HOUR}
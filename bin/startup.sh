#!/bin/bash
# Garlic 短链接服务启动脚本
APP_NAME="short-link-service"
JAR_PATH="short-link-api/target/short-link-api.jar"

# JVM 参数
JAVA_OPTS="-server \
  -Xms2g -Xmx2g -Xmn1g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=100 \
  -XX:G1HeapRegionSize=16m \
  -XX:+ParallelRefProcEnabled \
  -XX:+PrintGCDetails \
  -Xlog:gc*:file=logs/gc.log:time,uptime,level,tags:filecount=5,filesize=10m \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=logs/heapdump.hprof \
  -Dspring.profiles.active=dev,sharding \
  -Dserver.port=8001"

echo "Starting $APP_NAME..."
nohup java $JAVA_OPTS -jar $JAR_PATH > logs/app.log 2>&1 &
echo $! > logs/app.pid
echo "$APP_NAME started, PID: $(cat logs/app.pid)"

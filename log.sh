#!/bin/bash
nginx_home=/opt/module/nginx
log_home=/home/atguigu/realtimedata
case $1 in
"start")
    #判断nginx是否正在执行
    if [[ -z "$(ps -ef | grep nginx | grep -v grep )" ]]; then
       echo "Hadoop102启动nginx"
       #启动nginx
       $nginx_home/sbin/nginx
    else
        echo "nginx已经启动"
    fi

    #启动三台设备的日志服务器
    for host in hadoop162 hadoop163 hadoop164 ; do


        echo "启动 $host 上的日志服务器"
        ssh $host "nohup java -jar $log_home/gmall-logger-0.0.1-SNAPSHOT.jar --server.port=8081 >/dev/null 2>&1 &"
    done

;;
"stop")
    #先停止日志服务器
    for host in hadoop162 hadoop163 hadoop164 ; do
        echo "关闭 $host 上的日志服务器"
        ssh $host "jps | awk '/gmall-logger/ {print \$1}' | xargs kill -9 "
    done
    #停止nginx
    echo "停止nginx服务器"
    $nginx_home/sbin/nginx -s stop
;;
*)
    echo "input args error,plz input 'start' or 'stop'"
;;
esac

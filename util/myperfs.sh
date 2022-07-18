mypid=$(ps -ef | grep parallelwork* | head -1 | awk '{print $2}')
perf stat -p $mypid

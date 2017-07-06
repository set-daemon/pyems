#!/bin/bash
cur_hostname=`hostname`
#workers_host=("odps1" "odps2" "odps3" "odps4" "odps5" "odps6" "odps7" "rtss1" "rtss2" "rtss3")
workers_host=("daemon-world")

worker_id=1
for host_name in ${workers_host[@]}; do
	if [ $host_name = $cur_hostname ]; then
		break
	fi
	worker_id=$((worker_id+1))
done
echo $worker_id
nohup python ./apps/worker.py $worker_id &

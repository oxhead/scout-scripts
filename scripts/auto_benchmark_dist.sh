#!/bin/bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
. "$DIR/config.sh"

wait_for_cluster

for workload in "$@"
do
    num_params=`echo $workload | wc -w`
    if (( ${num_params} != 5 )); then echo Skip workload due to wrong format: ${workload}; continue; fi

    benchmark=`echo ${workload} | cut -d' ' -f1`
    framework=`echo ${workload} | cut -d' ' -f2`
    app=`echo ${workload} | cut -d' ' -f3`
    datasize=`echo ${workload} | cut -d' ' -f4`
    run_id=`echo ${workload} | cut -d' ' -f5`
    enable_upload=1
    timeout=7200  # 2 hours

    config_cluster
    start_cluster
    profile_app ${benchmark} ${framework} ${app} ${datasize} ${run_id} ${enable_upload} ${timeout}
done

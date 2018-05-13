#!/bin/bash

instance_type=`curl -s http://169.254.169.254/latest/meta-data/instance-type`
instance_id=`curl -s http://169.254.169.254/latest/meta-data/instance-id`

s3_bucket=s3://osr-mybenchmark-hibench
NUM_CORES=`getconf _NPROCESSORS_ONLN`

# assume to be under the osr user
source ~/project-aws/init.sh

#output_launched=/tmp/hibench_init_${instance_type}_${instance_id}.launched
#touch ${output_launched}
#aws s3 cp ${output_launched} ${s3_bucket}

# passed by userdata in CFT
workload_list="$1"
datasize_list="$2"
iterations="${3:-1}"  # with default value 1

echo "workload list: ${workload_list}"
echo "datasize list: ${datasize_list}"
echo "iterations: ${iterations}"


prewarm_system() {
    echo "Prewarm the system"
    local prewarm_datasize="warmup"
    myhibench auto_configure --map_parallelism 8 --shuffle_parallelism 8
    myhibench init
    myhibench start
    sleep 10  # wait for YARN/HDFS to be ready
    # warmup_workload_list="wordcount.spark wordcount.hadoop terasort.spark terasort.hadoop pagerank.spark pagerank.hadoop kmeans.spark kmeans.hadoop bayes.spark bayes.hadoop lr.spark als.spark scan.spark scan.hadoop aggregation.spark aggregation.hadoop join.spark join.hadoop"
    for workload in ${workload_list};
    do
        workload_name=`echo ${workload} | cut -d'.' -f1`
        framework=`echo ${workload} | cut -d'.' -f2`
        workload_output=/tmp/prewarm_${workload_name}_${framework}_${prewarm_datasize}_1
        echo myhibench run --workload ${workload} --datasize ${prewarm_datasize} --output_dir ${workload_output} --prepare --monitoring
        myhibench run --workload ${workload} --datasize ${prewarm_datasize} --output_dir ${workload_output} --prepare --monitoring
    done
}

profile_apps() {
    echo "Profile HiBench Applications"
    myhibench auto_configure --map_parallelism 8 --shuffle_parallelism 8
    myhibench init
    myhibench start

    sleep 10  # wait for YARN/HDFS to be ready

    for workload in ${workload_list};
    do
        workload_name=`echo ${workload} | cut -d'.' -f1`
        framework=`echo ${workload} | cut -d'.' -f2`
        for datasize in ${datasize_list};
        do
            # prepare dataset
            myhibench prepare_dataset --workload ${workload_name} --datasize ${datasize}
            for (( i=1;i<=${iterations};i++ ));
            do
                output_name=${instance_type}_${instance_id}_${workload_name}_${framework}_${datasize}_${i}
                workload_output=/tmp/${output_name}
                # 2 hours should be longer enough
                timeout 7200s myhibench run --workload ${workload} --datasize ${datasize} --output_dir ${workload_output} --monitoring
                # upload everything even with failures
                aws s3 sync ${workload_output} ${s3_bucket}/${output_name}
            done
        done
    done
}

prewarm_system
profile_apps

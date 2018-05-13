#!/bin/bash

instance_type=`curl -s http://169.254.169.254/latest/meta-data/instance-type`
instance_id=`curl -s http://169.254.169.254/latest/meta-data/instance-id`

s3_bucket=s3://osr-mybenchmark-sparkperf
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
    mysparkperf auto_configure
    mysparkperf init
    mysparkperf start

    sleep 10  # wait for YARN/HDFS to be ready

    # run the workload
    # warmup_workload_list="regression classification naive-bayes decision-tree random-forest gradient-boosted-tree als kmeans gmm lda pic svd pca summary-statistics block-matrix-mult pearson spearman chi-sq-feature chi-sq-gof word2vec fp-growth prefix-span"
    for workload in ${workload_list};
    do
        output_name=${instance_type}_${instance_id}_${workload}_spark1.5_warmup
        workload_output=/tmp/${output_name}
        mysparkperf run --instance ${instance_type} --workload ${workload} --datasize warmup --output_dir ${workload_output} --monitoring
    done
}

profile_apps() {
    echo "Profile spark-perf Applications"
    mysparkperf auto_configure
    mysparkperf init
    mysparkperf start

    sleep 10  # wait for YARN/HDFS to be ready
    
    # run the workload
    for (( i=1;i<=${iterations};i++ ));
    do
        for datasize in ${datasize_list};
        do
            for workload in ${workload_list};
            do
                output_name=${instance_type}_${instance_id}_${workload}_spark1.5_${datasize}_${i}
                workload_output=/tmp/${output_name}
                timeout 7200s mysparkperf run --instance ${instance_type} --workload ${workload} --datasize ${datasize} --output_dir ${workload_output} --monitoring
                # upload everything even with failures
                aws s3 sync ${workload_output} ${s3_bucket}/${output_name}
            done
        done
    done
}

prewarm_system
profile_apps

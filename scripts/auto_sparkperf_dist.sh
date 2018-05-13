#!/bin/bash

instance_type=`curl -s http://169.254.169.254/latest/meta-data/instance-type`
instance_id=`curl -s http://169.254.169.254/latest/meta-data/instance-id`
region=`curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region`
node_ip=`curl -s http://169.254.169.254/latest/meta-data/local-ipv4`

s3_bucket=s3://osr-mybenchmark-sparkperf-dist
NUM_CORES=`getconf _NPROCESSORS_ONLN`

# retrieve the cluster setting
fleet_request_id=`aws ec2 describe-tags --region ${region} --filters "Name=resource-id,Values=${instance_id}" | jq -r '.Tags[] | select(.Key=="aws:ec2spot:fleet-request-id") | .Value'`
echo $fleet_request_id
instance_list=`aws ec2 describe-spot-fleet-instances --spot-fleet-request-id ${fleet_request_id}  --region ${region} | jq -r '.ActiveInstances[].InstanceId' | xargs -I {} aws ec2 describe-instances --instance-id {} --region ${region} | jq -r '.Reservations[].Instances[].PrivateIpAddress' | sort | paste -sd " " -`
echo $instance_list

master=`echo ${instance_list} | awk '{print $1;}'`
echo $master
if [ "${node_ip}" != "${master}" ]; then
    echo "Not the master node"
    exit
fi

target_cluster_size=`aws ec2 describe-tags --region ${region} --filters "Name=resource-id,Values=${instance_id}" | jq -r '.Tags[] | select(.Key=="cluster-size") | .Value'`
cluster_mode=`aws ec2 describe-tags --region ${region} --filters "Name=resource-id,Values=${instance_id}" | jq -r '.Tags[] | select(.Key=="cluster-mode") | .Value'`
cluster_mode='n+1'
if [ "${cluster_mode}" == "n+1" ]; then
    cluster_size=$((target_cluster_size-1))
else
    cluster_size=${target_cluster_size}
fi
if [ "${cluster_mode}" == "n+1" ]; then
    node_list_before=( ${instance_list} )
    node_list="${node_list_before[@]:1}"
else
    node_list=(${instance_list})
fi
master=`echo ${instance_list} | awk '{print $1;}'`
echo $master
echo "Slaves: ${node_list}"

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


wait_for_cluster() {
    while true;
    do
        current_cluster_size=`aws ec2 describe-spot-fleet-instances --spot-fleet-request-id ${fleet_request_id}  --region ${region} | jq -r '.ActiveInstances' | jq length`
        echo "target cluster size: ${target_cluster_size}"
        echo "current cluster size: ${current_cluster_size}"
        if [ "${target_cluster_size}" == "${current_cluster_size}" ];
        then
            # better?
            echo "Cluster is ready, waiting for 60 more seconds"
            sleep 60
            break
        fi
        sleep 30
    done
}

config_cluster() {
    for node_ip in ${instance_list};
    do
        echo "Configuring ${node_ip}"
        #ssh ${node_ip} "source ~/project-aws/init.sh; mysparkperf_dist auto_configure --master ${master} --slaves '${node_list}'"
        ssh ${node_ip} "source ~/project-aws/init.sh; mysparkperf_dist auto_configure --master ${master} --slaves '${node_list}'; myhibench_dist init"
    done
    ssh ${master} "source ~/project-aws/init.sh; mysparkperf_dist start"
}

prewarm_system() {
    echo "Prewarm the system"
    sleep 10  # wait for YARN/HDFS to be ready

    # run the workload
    # warmup_workload_list="regression classification naive-bayes decision-tree random-forest gradient-boosted-tree als kmeans gmm lda pic svd pca summary-statistics block-matrix-mult pearson spearman chi-sq-feature chi-sq-gof word2vec fp-growth prefix-span"
    for workload in ${workload_list};
    do
        output_name=${cluster_size}_${instance_type}_${instance_id}_${workload}_spark1.5_warmup
        workload_output=/tmp/${output_name}
        mysparkperf_dist run --mode ${cluster_mode} --slaves "${node_list}" --workload ${workload} --datasize warmup --output_dir ${workload_output} --monitoring
    done
}

profile_apps() {
    echo "Profile spark-perf Applications"
    # sleep 10  # wait for YARN/HDFS to be ready
    
    # run the workload
    for (( i=1;i<=${iterations};i++ ));
    do
        for datasize in ${datasize_list};
        do
            for workload in ${workload_list};
            do
                output_name=${cluster_size}_${instance_type}_${instance_id}_${workload}_spark1.5_${datasize}_${i}
                workload_output=/tmp/${output_name}
                timeout 7200s mysparkperf_dist run --mode ${cluster_mode} --slaves "${node_list}" --workload ${workload} --datasize ${datasize} --output_dir ${workload_output} --monitoring
                # upload everything even with failures
                aws s3 sync ${workload_output} ${s3_bucket}/${output_name}
                sar_data_path="${workload_output}/sar*.csv"
                for node_ip in ${node_list};
                do
                    echo "Uploading sar data from ${node_ip}"
                    cmd=`echo "source ~/project-aws/init.sh; aws s3 cp ${sar_data_path} ${s3_bucket}/${output_name}/" | base64 -w0`
                    #echo $cmd
                    echo ssh ${node_ip} `echo $cmd | base64 -d`
                    ssh ${node_ip} "echo $cmd | base64 -d | bash"
                done
            done
        done
    done
}

#wait_for_cluster
#config_cluster
#prewarm_system
#config_cluster
profile_apps

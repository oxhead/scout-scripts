#!/bin/bash

instance_type=`curl -s http://169.254.169.254/latest/meta-data/instance-type`
instance_id=`curl -s http://169.254.169.254/latest/meta-data/instance-id`
region=`curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region`
node_ip=`curl -s http://169.254.169.254/latest/meta-data/local-ipv4`

# retrieve the cluster setting
fleet_request_id=`aws ec2 describe-tags --region ${region} --filters "Name=resource-id,Values=${instance_id}" | jq -r '.Tags[] | select(.Key=="aws:ec2spot:fleet-request-id") | .Value'`
echo "Fleet:" $fleet_request_id
instance_list=`aws ec2 describe-spot-fleet-instances --spot-fleet-request-id ${fleet_request_id}  --region ${region} | jq -r '.ActiveInstances[].InstanceId' | xargs -I {} aws ec2 describe-instances --instance-id {} --region ${region} | jq -r '.Reservations[].Instances[].PrivateIpAddress' | sort | paste -sd " " -`
echo "Instance list:" $instance_list

master=`echo ${instance_list} | awk '{print $1;}'`
echo "Master:" $master
if [ "${node_ip}" != "${master}" ]; then
    echo "Not the master node"
    exit
fi

target_cluster_size=`aws ec2 describe-tags --region ${region} --filters "Name=resource-id,Values=${instance_id}" | jq -r '.Tags[] | select(.Key=="cluster-size") | .Value'`
cluster_mode=`aws ec2 describe-tags --region ${region} --filters "Name=resource-id,Values=${instance_id}" | jq -r '.Tags[] | select(.Key=="cluster-mode") | .Value'`
cluster_mode="${cluster_mode:-n+1}"
echo "Cluster mode:" $cluster_mode
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
echo "Slaves: ${node_list}"

s3_bucket=`aws ec2 describe-tags --region ${region} --filters "Name=resource-id,Values=${instance_id}" | jq -r '.Tags[] | select(.Key=="s3-bucket") | .Value'`

##########################
# functions
##########################

terminate_fleet() {
    aws ec2 --region ${region} cancel-spot-fleet-requests --spot-fleet-request-ids "${fleet_request_id}" --terminate-instances --no-dry-run
}

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
        (ssh ${node_ip} "myhibench auto_configure --master ${master} --slaves '${node_list}'; myhibench init") &
    done
    wait
    echo All instances are configured
}

start_cluster() {
    ssh ${master} "myhibench start"
}

profile_app() {
    if (( $# < 5 )); then
        echo Incorrect parameters: $@
        exit 1
    fi
    benchmark=$1
    framework=$2
    app=$3
    datasize=$4
    run_id=$5
    enable_upload=${6:-0} # 1=true, 0=false
    timeout=${7:-7200}

    script_name="my${benchmark}"

    echo "*******************************"
    echo "Profiling application"
    echo "*******************************"
    echo - Benchmark: ${benchmark}
    echo - Framework: ${framework}
    echo - Application: ${app}
    echo - Datasize: ${datasize}
    echo - Run Id: ${run_id}
    echo - Profiling: ${enable_upload}
    echo - Timeout: ${timeout}

    # prepare dataset
    if [ "${benchmark}" == "hibench" ]; then
        ${script_name} prepare_dataset --workload ${app} --datasize ${datasize}
    fi
                
    output_name=${cluster_size}_${instance_type}_${app}_${framework}_${datasize}_${run_id}
    workload_output=/tmp/${output_name}

    # 2 hours should be longer enough
    # timeout should be done in the python script, otherwise, data won't be collected
    if [ "${benchmark}" == "hibench" ]; then
        workload_id="${app}.${framework}"
    else
        workload_id="${app}"
    fi

    ${script_name} run --timeout ${timeout} --mode ${cluster_mode} --slaves "${node_list}" --workload ${workload_id} --datasize ${datasize} --output_dir ${workload_output} --monitoring
       
    # upload everything even with failures
    if (( $enable_upload > 0 )); then
        aws s3 sync ${workload_output} s3://${s3_bucket}/${output_name}
        if [ "${benchmark}" == "hibench" ]; then
            sar_data_path="/opt/HiBench/report/${app}/${framework}/sar*.csv"
        else
            sar_data_path="${workload_output}/sar*.csv"
        fi
        for node_ip in ${node_list};
        do
            echo "Uploading sar data from ${node_ip}"
            cmd=`echo "aws s3 cp ${sar_data_path} s3://${s3_bucket}/${output_name}/" | base64 -w0`
            echo ssh ${node_ip} `echo $cmd | base64 -d`
            (ssh ${node_ip} "echo $cmd | base64 -d | bash") &
        done
        wait
        echo All sar data is uploaded to S3
    fi
}

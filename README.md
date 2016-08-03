<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [avada-bench](#avada-bench)
    - [Requirements](#requirements)
    - [How to start?](#how-to-start)
    - [Prepare the dataset from Amazon S3](#prepare-the-dataset-from-amazon-s3)
    - [Run the benchmark scripts and collect monitoring data](#run-the-benchmark-scripts-and-collect-monitoring-data)
    - [How to parse the results](#how-to-parse-the-results)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# avada-bench
Scripts for running Big Data Benchmark on existing Spark SQL cluster

### Requirements
    * Install ansible (>= 1.9) on master node
    * copy hostname of all hosts to `/etc/ansible/hosts`

### How to start?
   The playbook in directory `playbook` will help in enable the `redis-server` and `monitor-daemon`. 
   
   Add the `hosts` file to the `playbook` directory first, and execute:
  
      ansible-playbook -i hosts worker init.yml

### Prepare the dataset from Amazon S3
    python prepare_benchmark.py --spark-master spark://SPARK_HOME:7077 -n 1
  
### Run the benchmark scripts and collect monitoring data
    python run.py -q 1a 
    
### How to parse the results
    python monitor_parser.py -p /path/to/monitor/


    sudo apt-get install byacc flex


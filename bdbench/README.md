<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [avada-bdbench](#avada-bdbench)
    - [Requirements](#requirements)
    - [Prepare the dataset from Amazon S3](#prepare-the-dataset-from-amazon-s3)
    - [Only run the benchmark scripts](#only-run-the-benchmark-scripts)
    - [Run the benchmark scripts and collect monitoring data](#run-the-benchmark-scripts-and-collect-monitoring-data)
    - [How to parse the results](#how-to-parse-the-results)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# avada-bdbench
Scripts for running Big Data Benchmark on existing Spark SQL cluster

### Requirements
    * Install ansible (>= 1.9) on master node
    * copy hostname of all hosts to `/etc/ansible/hosts`
    
### Prepare the dataset from Amazon S3
    python prepare_benchmark.py --spark-master spark://SPARK_HOME:7077 -n 1
  
### Only run the benchmark scripts
    python run.py -q 1a 

### Run the benchmark scripts and collect monitoring data
    python run.py -q 1a 
    
### How to parse the results
    python monitor_parser.py -p /path/to/monitor/

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


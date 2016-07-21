#!/bin/python
__author__ = "www.haow.ca"

# require: ansible
# Todo: refactor with ansible python lib
# Todo: cleanup ansible processes when exceptional interrupt

import os
import sys
import signal
import subprocess
from argparse import ArgumentParser
import time
import datetime

BASE_DIR = "~/monitor/"
MASTER = "ec2-54-224-95-250.compute-1.amazonaws.com"


def parse_args():
    parser = ArgumentParser(usage="run.py [options]")

    parser.add_argument("--node", dest="node", type=str, default="worker",  
                        help="collect results from which host (default: all)")
    parser.add_argument("-q", "--query-num", default="1a", 
                        help="Which query to run in benchmark")
    parser.add_argument("-n", "--trial-num", default="1", 
                        help="Repeat times of executing query")
    parser.add_argument("--hosts", dest="hosts", type=str, 
                        default="./conf/hosts", 
                        help="host list, format: 10.2.3.4 hao-spark-1")
    parser.add_argument("--spark-master", dest="spark_master",
                        default=os.environ.get("SPARK_MASTER"), 
                        help="Address of Sparl master")
    opts = parser.parse_args()

    if opts.hosts is None:
        print opts.print_help()

    if opts.spark_master:
        MASTER = opts.spark_master 

    return opts


def execute(command):
    return subprocess.check_call(command, shell=True)


def ansible_exe(host, command):
    # os.setsid creates a new process session
    # all descendant processes will be added into this session
    # then we could kill them all by process group id
    return subprocess.Popen("ansible %s -m shell -a  \"%s async=45 poll=5\"" % (
        host, command), shell=True, preexec_fn=os.setsid)


def pre_run(opts, prefix):
    try:
        # create local monitor directory
        print "####Create local monitor directory####"
        execute("mkdir -p " + BASE_DIR + prefix)
        # create remote monitor directory
        print "####Create remote monitor directory####"
        p = ansible_exe(opts.node, "mkdir -p %s/{{inventory_hostname}}" % (
           BASE_DIR + prefix))
        p.wait()

        print "####Launch Redis Server####"
        p = ansible_exe(opts.node, "./monitor_daemon.py &")
        p.wait()

    except subprocess.CalledProcessError as e:
        return e.returncode


def post_run(opts, prefix):
    # parse data from redis-server

    # create a tar package to include results of this time
    print "####Build packge of remote results####"
    cmd = "tar -cf %s/%s/{{inventory_hostname}}.tar " % (BASE_DIR, prefix) \
        + "-C %s/ {{inventory_hostname}}" % (BASE_DIR + prefix)
    p = ansible_exe(opts.node, cmd)
    p.wait()

    # remote fetch
    print "####Fetch remote reulst packges####"
    cmd = "src=%s%s/{{inventory_hostname}}.tar dest=%s%s/ " % (
        BASE_DIR, prefix, BASE_DIR, prefix) + "flat=yes validate_checksum=no"
    p = subprocess.Popen("ansible %s -m fetch -a \"%s\"" % (
        opts.node, cmd), shell=True)
    p.wait()

    # local tar package extract
    print "####Extract reulst packges####"
    cmd = "cat %s/*.tar | tar -xf - -i -C %s/" % (
        BASE_DIR + prefix, BASE_DIR + prefix)
    execute(cmd)

    # copy spark.log to direcotry
    print "####Move spark.log to monitoring directory####"
    execute("mv ~/spark-1.6.1/logs/spark.log " + BASE_DIR + prefix + "/")

    # delete all tar packages
    print "####Remote reulst tar packges####"
    cmd = "rm %s/*.tar" % (BASE_DIR + prefix)
    execute(cmd)


def run(opts):
    prefix = str(time.time()).split(".")[0]
    pre_run(opts, prefix)

    start_time = prefix
    cmd = "python avada-ML/monitor-daemon.py"
    p = ansible_exe(opts.node, cmd)
    
    print "####Run Spark SQL query %s####" % opts.query_num
    run_query = ("python run_query.py --spark-master spark://%s:7077" 
                 " -q %s --num-trials %s") % (
                         MASTER, opts.query_num, opts.trial_num)

    query_p = subprocess.Popen(run_query, shell=True)
    query_p.wait()
    
    print "####Finish Spark SQL query####"
    end_time = time.time()

    for p in monitor_proc:
        print "Killing: \"%s\"" % run_monitor[monitor_proc.index(p)]
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
    
    print start_time, end_time
    post_run(opts, prefix, start_time, end_time)


def main():
    opts = parse_args()
    try:
        run(opts)
    except KeyboardInterrupt:
        print "Ctrl-C interrupt, kill all monitoring processes"
        ansible_exe(opts.node, "sudo killall iostat nethogs jvmtop.sh")


if __name__ == "__main__":
    main()

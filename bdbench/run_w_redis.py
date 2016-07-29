#!/usr/bin/python
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
MASTER = "ec2-54-174-214-131.compute-1.amazonaws.com"


def parse_args():
    parser = ArgumentParser(usage="run.py [options]")

    parser.add_argument("--node", dest="node", type=str, default="worker",  
                        help="collect results from which host (default: worker)")
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

    if opts.hosts is None or opts.spark_master is None:
        print opts.print_help()
        exit(0)
    MASTER = opts.spark_master 

    return opts


def execute(command):
    return subprocess.check_call(command, shell=True)


def ansible_exe(host, command):
    # os.setsid creates a new process session
    # all descendant processes will be added into this session
    # then we could kill them all by process group id
    p = subprocess.Popen("ansible %s -m shell -a  \"%s async=45 poll=5\"" % (
        host, command), shell=True, preexec_fn=os.setsid)
    p.wait()


def pre_run(opts, prefix):
    try:
        # create local monitor directory
        print "----Create local monitor directory----"
        execute("mkdir -p " + BASE_DIR + prefix)

        # create remote monitor directory
        print "----Create remote monitor directory----"
        ansible_exe(opts.node, "mkdir -p %s/" % (
           BASE_DIR + prefix))

        print "----Launch monitoring daemon----"
        ansible_exe(opts.node, "python /home/ubuntu/monitor_daemon.py &")

    except subprocess.CalledProcessError as e:
        return e.returncode


def post_run(opts, prefix, start_time, end_time):
    # kill monitor daemon
    print "----Kill monitor daemon process----"
    ansible_exe(opts.node, "sudo pkill python")

    # parse data from redis-server
    cmd = ("python /home/ubuntu/redis_to_file.py -s %s"
            " -e %s -d {{ inventory_hostname }} -q % s") \
                    % (start_time, end_time, opts.query_num)
    ansible_exe(opts.node, cmd)

    # remote fetch
    print "----Fetch remote reulst packges----"
    cmd = "scp ~/monitor/%s-* {{ groups['master'][0] }}:%s%s" \
        % (start_time, BASE_DIR, prefix)
    ansible_exe(opts.node, cmd)

    # local tar package extract
    print "----Extract reulst packges----"
    cmd = "cat %s/*.tar | tar -xf - -i -C %s/" % (
        BASE_DIR + prefix, BASE_DIR + prefix)
    execute(cmd)

    # delete all tar packages
    print "----Remote reulst tar packges----"
    cmd = "rm %s/*.tar" % (BASE_DIR + prefix)
    execute(cmd)


def run(opts):
    prefix = str(time.time()).split(".")[0]
    pre_run(opts, prefix)

    print "----Run Spark SQL query %s----" % opts.query_num
    run_query = ("python run_query.py"
                 " --spark-master spark://%s:7077" 
                 " -q %s --num-trials %s") % (
                     MASTER, opts.query_num, opts.trial_num)

    query_p = subprocess.Popen(run_query, shell=True)
    query_p.wait()
    
    print "----Finish Spark SQL query----"
    end_time = str(time.time()).split(".")[0]

    print start_time, end_time
    post_run(opts, prefix, start_time, end_time)


def main():
    opts = parse_args()
    try:
        run(opts)
    except KeyboardInterrupt:
        print "Ctrl-C interrupt, kill all monitoring processes"
        ansible_exe(opts.node, "sudo pkill python")


if __name__ == "__main__":
    main()

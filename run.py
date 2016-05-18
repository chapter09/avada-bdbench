#!/bin/python
__author__  = "www.haow.ca"

#require: ansible

import os, sys
import signal
import subprocess
from argparse import ArgumentParser
import time, datetime

BASE_DIR = "~/monitor/"


def parse_args():
	parser = ArgumentParser(usage="run.py [options]")

	parser.add_argument("--host", dest="host", type=str,
			default="all", help="collect results from which host (default: all)")
	parser.add_argument("-q", "--query-num", default="1a",
			help="Which query to run in benchmark")

	opts = parser.parse_args()

	return opts


def execute(command):
	return subprocess.check_call(command, shell=True)


def ansible_exe(host, command):
	# os.setsid creates a new process session
	# all descendant processes will be added into this session
	# then we could kill them all by process group id
	return subprocess.Popen("ansible %s -m shell -a  \"%s\"" % (
		host, command), shell=True, preexec_fn=os.setsid)


def pre_run(opts, prefix):
	# clean up SSH process

	# create the monitor directory
	cmd = "ansible %s -m shell -a \"mkdir -p %s/%s\"" % ( \
			opts.host, BASE_DIR, prefix)
	try:
		execute(cmd)
	except subprocess.CalledProcessError as e:
		return e.returncode


def run(opts):
	prefix = str(time.time()).split(".")[0]
	pre_run(opts, prefix)
	
	monitor_proc = []

	##Todo: remove hardcode
	run_query = ("python run_query.py --hadoop ~/hadoop-2.6.4"
		 " --spark ~/spark-1.6.1 --spark-master spark://10.2.3.4:7077"
		 " -q %s --num-trials 1") % opts.query_num
	query_p = subprocess.Popen(run_query, shell=True)

	run_monitor = [
			#run_disk_monitor
			"iostat -x 1 > %s/disk-query-%s.log" % (BASE_DIR+prefix, opts.query_num),		
			#run_net_monitor
			("export JAVA_HOME=/usr/lib/jvm/java-8-oracle/;"
				"./jvmtop/jvmtop.sh > %s/jvmtop-query-%s.log") % (BASE_DIR+prefix, \
						opts.query_num),	
			##Todo: hardcode eth0
			#run_jvm_monitor
			"sudo nethogs eth0 -t > %s/net-query-%s.log" % (BASE_DIR+prefix, \
					opts.query_num)
			]

	time.sleep(10)
	print "Waiting for Spark SQL warming up..."

	for cmd in run_monitor:
		print "Executing: \"%s\"" % cmd
		monitor_proc.append(ansible_exe(opts.host, cmd))
		time.sleep(1)

	query_p.wait()

	for p in monitor_proc:
		print "Killing: \"%s\"" % run_monitor[monitor_proc.index(p)]
		os.killpg(os.getpgid(p.pid), signal.SIGKILL)


def collect():
	pass


def main():
	opts = parse_args()
	run(opts)


if __name__ == "__main__":
	main()

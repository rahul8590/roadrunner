#!/usr/bin/python
import os
import json
import logging
import sys
import subprocess
from multiprocessing import Pool


#
# Global variables
#
l = None  # Logger variable


#
# Initialize log level and return the logger object
#
def set_logger(log_level):
	global l

	l = logging.getLogger('roadrunner')
	handler = logging.StreamHandler()
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	handler.setFormatter(formatter)
	l.addHandler(handler)
	l.setLevel(log_level)


#
# Read the job flow json file
# and return the constructed object
#
def get_job_flow_config(json_file):
	global l

	job_flow_config = None
	try:
		f = open(os.path.abspath(json_file), 'r')
		job_flow_config = json.load(f)
		
	except IOError as (errno, errstr):
		l.error("Error opening jobflow file " + json_file)
		sys.exit(1)

	except:
		l.error("Error decoding the jobflow file " + json_file)
		f.close()
		sys.exit(1)

	return job_flow_config


#
# Run a fabric job with the given arguments
#
def run_fabric_job(hosts, cmd, retries, timeout, parallelism):
	global l

	# Build the fabric command
	fabric_cmd = "fab -H " + ",".join(hosts) + " -f ./tests/test_fabfile.py --linewise"
	if(parallelism):
		fabric_cmd += " -z " + str(parallelism)
	fabric_cmd += " runcmd:'" + cmd + "'"

	l.debug("Running fabric command: " + fabric_cmd)

	# Run the command as a subprocess and get it's output
	# The stderr is redirected to the stdout
	p = subprocess.Popen(fabric_cmd, bufsize=2048, shell=True,
			stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
	p.wait()
	output = p.stdout.read()
	
	# Stream the output to the plugin
	pluginp = subprocess.Popen("./plugins/simpleout.py", bufsize=2048, shell=True,
			stdin=subprocess.PIPE)
	pluginp.communicate(output)

#
# Run the jobs according to the job flow
# Use fabric to run the jobs
#
def run_jobs(job_flow_config):
	global l

	flow = job_flow_config['job_flow'];
	for slot in flow:
		job_pool = Pool()
		jobs = slot['jobs']
		for job in jobs:
			job_id = job['job_id']
			cmd = job['cmd']
			hosts = job['hosts']
			if(job.has_key('timeout')):
				timeout = job['timeout']
			else:
				timeout = job_flow_config['default_job_timeout']
			if(job.has_key('retries')):
				retries = job['retries']
			else:
				retries = job_flow_config['default_retries']
			if(job.has_key('parallelism')):
				parallelism = job['parallelism']
			else:
				parallelism = None
			success_constraint = job['success_constraint']

			l.debug("slot: " + str(slot['slot']) + ", job_id: " + job['job_id'] +
			", timeout: " + str(timeout) + ", retries: " + str(retries) +
			", success_constraint: " + success_constraint +
			", parallelism: " + str(parallelism) + ", cmd: " + cmd + ", hosts: " + str(hosts))
	
			# Add jobs to the pool (run the actual fabric commands)
			job_pool.apply_async(run_fabric_job, (hosts, cmd, retries, timeout, parallelism))

		job_pool.close()
		job_pool.join()


#
# main function
#
def main():
	set_logger(logging.DEBUG)
	job_flow_config = get_job_flow_config("./tests/test_flow.json")
	run_jobs(job_flow_config)


if __name__ == "__main__":
	main()

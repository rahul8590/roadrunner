#!/usr/bin/python
import os
import json
import logging
import sys


#
# Global variables
#
l = None  # Logger variable
plugin_directory = "./plugins"


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
# Run the jobs according to the job flow
#
def run_jobs(job_flow_config):
	global l

	output_plugin = job_flow_config['output_plugin']
	flow = job_flow_config['job_flow']
	for slot in flow:
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


#
# main function
#
def main():
	set_logger(logging.DEBUG)
	job_flow_config = get_job_flow_config("./tests/test_flow.json")
	run_jobs(job_flow_config)


if __name__ == "__main__":
	main()

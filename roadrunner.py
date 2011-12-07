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
# Check to see if a key exists in a dictionary
# If yes, return it's value or return None
#
def get_dict_val(key, dic):
	if(dic.has_key(key)):
		return dic[key]
	return None


#
# Run the jobs according to the job flow
#
def run_jobs(job_flow_config):
	global l

	# Mandatory fields required in a job flow config
	output_plugin = get_dict_val('output_plugin', job_flow_config)
	flow = get_dict_val('job_flow', job_flow_config)
	default_timeout = get_dict_val('default_job_timeout', job_flow_config)
	default_retries = get_dict_val('default_retries', job_flow_config)

	if(not (output_plugin and flow and default_timeout and default_retries)):
		l.error("There is an error in your job flow config file. Please ensure that all mandatory fields are present")
		sys.exit(1)

	for slot in flow:
		# Mandatory fields for slot
		jobs = get_dict_val('jobs', slot)
		slot_id = get_dict_val('slot_id', slot)

		if(not (jobs and slot_id)):
			l.error("There is an error in your job flow config file. Please ensure that all mandatory fields are present")
			sys.exit(1)

		for job in jobs:
			# Mandatory fields that need to be present (for a job)
			job_id = get_dict_val('job_id', job)
			cmd = get_dict_val('cmd', job)
			hosts = get_dict_val('hosts', job)

			if(not (job_id and cmd and hosts)):
				l.error("There is an error in your job flow config file. Please ensure that all mandatory fields are present")
				sys.exit(1)

			# Optional params
			timeout = get_dict_val('timeout', job)
			if(timeout == None):
				timeout = default_timeout
			retries = get_dict_val('retries', job)
			if(retries == None):
				retries = default_retries
			success_constraint = get_dict_val('success_constraint', job)
			if(success_constraint == None):
				success_constraint = "100%"
			parallelism = get_dict_val('parallelism', job)

			l.debug("slot_id: " + slot_id + ", job_id: " + job_id +
			", timeout: " + timeout + ", retries: " + retries +
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

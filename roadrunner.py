#!/usr/bin/python
import os
import json
import logging
import sys
import argparse


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
	if(log_level):
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
def get_dict_val(key, dic, exit_on_error=False):
	global l
	if(dic.has_key(key)):
		return dic[key]
	else:
		if(exit_on_error):
			l.error("Key: " + key + " not found. Please make sure it is present!")
			sys.exit(1)
		else:
			return None


#
# Run the jobs according to the job flow
#
def run_jobs(job_flow_config):
	global l

	# Mandatory fields required in a job flow config
	output_plugin = get_dict_val('output_plugin', job_flow_config, True)
	flow = get_dict_val('job_flow', job_flow_config, True)
	default_timeout = get_dict_val('default_job_timeout', job_flow_config, True)
	default_retries = get_dict_val('default_retries', job_flow_config, True)

	for slot in flow:
		# Mandatory fields for slot
		jobs = get_dict_val('jobs', slot, True)
		slot_id = get_dict_val('slot_id', slot, True)

		for job in jobs:
			# Mandatory fields that need to be present (for a job)
			job_id = get_dict_val('job_id', job, True)
			cmd = get_dict_val('cmd', job, True)
			hosts = get_dict_val('hosts', job, True)

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
# Parse commandline arguments
#
def parse_args():
	global l
	args = sys.argv
	parser = argparse.ArgumentParser()
	parser.add_argument('--jobflow', action='store', nargs=1, help='The path to the jobflow config file')
	parser.add_argument('--debug', action='store_true', default=None, help='Print debug output')
	return parser.parse_args(args[1:])


#
# main function
#
def main():
	pargs = parse_args() # Args after being parsed

	# Check to see if --debug is specified
	if(pargs.debug):
		log_level = logging.DEBUG
	else:
		log_level = None
	set_logger(log_level)

	# Check if all the mandatory options/arguments are specified or not
	if(not pargs.jobflow):
		l.error("Please specify the jobflow file path")
		sys.exit(1)
	
	job_flow_config = get_job_flow_config(pargs.jobflow[0])
	run_jobs(job_flow_config)


if __name__ == "__main__":
	main()

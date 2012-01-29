#!/usr/bin/env python
import os
import json
import logging
import sys
import argparse
from multiprocessing import Process
from job import Job
from logger import Logger


#
# Global logger variable
#
l = Logger('ROADRUNNER').get()


#
# Read the job flow json file
# and return the constructed object
#
def get_job_flow_config(json_file):
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
    if dic.has_key(key):
        return dic[key]
    else:
        if exit_on_error:
            l.error("Key: " + key + " not found. Please make sure it is present!")
            sys.exit(1)
        else:
            return None


#
# The function that will be run by the thread
# The thread will execute a job and wait for it to complete
#
def subprocess_wrapper(j):
    j.run()
    j.poll()
    if j.success():
        l.debug("Job: " + j._job_id + " executed successfully!")
    else:
        l.error("Job: " + j._job_id + " failed!")
        sys.exit(1)


#
# Run the jobs according to the job flow
#
def run_jobs(job_flow_config):
    # Mandatory fields required in a job flow config
    output_plugin = get_dict_val('output_plugin', job_flow_config, True)
    flow = get_dict_val('job_flow', job_flow_config, True)
    default_timeout = get_dict_val('default_job_timeout', job_flow_config, True)
    default_retries = get_dict_val('default_retries', job_flow_config, True)

    for slot in flow:
        process_pool = []
        jobs = get_dict_val('jobs', slot, True)
        slot_id = get_dict_val('slot_id', slot, True)
        
        for job in jobs:
            
            # Mandatory fields that need to be present (for a job)
            job_id = get_dict_val('job_id', job, True)
            cmd = get_dict_val('cmd', job, True)
            hosts = get_dict_val('hosts', job, True)

            # Optional params
            timeout = get_dict_val('timeout', job)
            if timeout == None:
                timeout = default_timeout
            retries = get_dict_val('retries', job)
            if retries == None:
                retries = default_retries
            success_constraint = get_dict_val('success_constraint', job)
            if success_constraint == None:
                success_constraint = "100%"
            parallelism = get_dict_val('parallelism', job)
            if parallelism == None:
                parallelism = "100%"

            j = Job(job_id, timeout, retries, success_constraint, parallelism, cmd, hosts)

            l.debug("slot_id: " + slot_id + ", job_id: " + job_id +
                    ", timeout: " + timeout + ", retries: " + retries +
                    ", success_constraint: " + success_constraint +
                    ", parallelism: " + str(parallelism) + ", cmd: " + cmd + ", hosts: " + str(hosts))

            # Spawn a thread per job
            try:
                p = Process(target=subprocess_wrapper, args=(j,))
                p.start()
                process_pool.append(p)

            except:
                l.error("Unable to spawn a process for job: " + j._job_id)
                sys.exit(1)


        # Wait for all the processes to complete
        for p in process_pool:
            try:
                p.join()

            except:
                l.error("An unexpected error occurred while fetching info from the subprocess!")
                sys.exit(1)


#
# Parse commandline arguments
#
def parse_args():
    args = sys.argv
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobflow', action='store', nargs=1, help='The path to the jobflow config file')
    return parser.parse_args(args[1:])


#
# main function
#
def main():
    pargs = parse_args() # Args after being parsed

    # Check if all the mandatory options/arguments are specified or not
    if not pargs.jobflow:
        l.error("Please specify the jobflow file path")
        sys.exit(1)
    
    job_flow_config = get_job_flow_config(pargs.jobflow[0])
    run_jobs(job_flow_config)

    sys.exit(0)


if __name__ == "__main__":
    main()

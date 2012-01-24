#!/usr/bin/env python
import sys
import json
import gearman
from fabric import *
from fabric.api import *
from gearman import GearmanWorker


#
# Run the ssh task
#
def exe_job(worker, job):
    d = json.loads(job.data)
    env.host_string = d['host'] 
    cmd = d['command']

    # Run the fabric command. Do not abort on exit
    with settings(warn_only=True):
        result = run(cmd)

    if result.failed:
        job.send_fail()

    return str(result)


#
# Main function
#
def main():
    gm_worker = gearman.GearmanWorker(['localhost:4730'])
    gm_worker.register_task('exe_job',exe_job) 
    gm_worker.work()


if __name__ == '__main__':
    main()

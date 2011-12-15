#!/usr/bin/env python
import os
import sys
import gearman

#
# Global variables
#
gearman_servers = ['localhost:4730']

#
# Job class
#
class Job:

    def __init__(self, job_id, timeout, retries, success_constraint,
            parallelism, command, hosts):
        self._job_id  = job_id
        self._timeout = int(timeout)
        self._retries = int(retries)
        self._success_constraint = success_constraint
        self._parallelism = parallelism
        self._command = command
        self._hosts = hosts
        self._rcs = {} # Individual return codes for each host
        self._output = {} # Ouputs per host in the job
        self._success = False
        self._gmjobs = []
        self._gmclient = None
        self._completed_gmjobs = []

    def run(self):
        global gearman_servers
        worker_found = False
        task_name = 'ls'

        # Check if there are a workers that have the ssh job registered
        # If not, bail out
        gmadmin = gearman.GearmanAdminClient(gearman_servers)
        stats = list(gmadmin.get_status())
        for stat in stats:
            if stat['task'] == task_name and stat['workers'] > 0:
                worker_found = True
                break

        if worker_found:
            print "DBG: Found atleast one worker with the task: " + task_name + " registered"
        else:
            print "ERR: Did not find any workers with the task: " + task_name + " registered"
            sys.exit(1)

        # Gearman client should now submit tasks to the gearman workers
        gmjobs = []
        for host in self._hosts:
            gmjobs.append(dict(task=task_name, data="hey " + str(host)))
        self._gmclient = gearman.GearmanClient(gearman_servers)
        self._gmjobs = self._gmclient.submit_multiple_jobs(gmjobs, wait_until_complete=False,
                                                     background=False, max_retries=self._retries)


    def poll(self):
        self._completed_gmjobs = self._gmclient.wait_until_jobs_completed(self._gmjobs, poll_timeout=self._timeout)
        for index, gmjob in enumerate(self._completed_gmjobs):
            host = self._hosts[index]
            if gmjob.state == gearman.job.JOB_COMPLETE:
                self._rcs[host] = 0
            elif gmjob.state == gearman.job.JOB_FAILED:
                self._rcs[host] = 1
            elif gmjob.state == gearman.job.JOB_UNKNOWN:
                self._rcs[host] = 2
            else:
                self._rcs[host] = 3

            self._output[host] = gmjob.result


    def success(self):
        if self._success_constraint[-1] == '%':
            num_hosts = (int(self._success_constraint[:-1]) * len(self._hosts)) / 100
        else:
            num_hosts = int(self._success_constraint)

        # Atleast one host should succeed
        if num_hosts == 0:
            num_hosts = 1

        # Check the status codes for each host
        success_count = 0
        for rc in self._rcs.values():
            if rc == 0:
                success_count = success_count + 1

        if success_count >= num_hosts:
            return True
        else:
            return False

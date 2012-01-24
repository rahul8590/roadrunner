#!/usr/bin/env python
import sys
import gearman
import json
from logger import Logger

#
# Global variables
#
gearman_servers = ['localhost:4730']
l = Logger('JOBMANAGER').get()


#
# Utility function to convert the pct value
# into number of hosts
#
def get_num_hosts(val, total):
    try:
        if val[-1] == '%':
            num_hosts = (int(val[:-1]) * total) / 100
        else:
            num_hosts = int(val)

        # Atleast one host should succeed
        if num_hosts <= 0:
            num_hosts = 1
                
        if num_hosts > total:
            num_hosts = total

    except ValueError:
        num_hosts = None

    return num_hosts


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
        self._rcs = {} # Individual return codes for each individual gearman job
        self._output = {} # Ouputs per individual gearman job
        self._success = False
        self._gmjobs = []
        self._gmclient = None
        self._completed_gmjobs = []


    def run(self):
        worker_found = False
        task_name = 'exe_job'

        try:
            # Check if there are a workers that have the ssh job registered
            # If not, bail out
            gmadmin = gearman.GearmanAdminClient(gearman_servers)
            stats = list(gmadmin.get_status())
            for stat in stats:
                if stat['task'] == task_name and stat['workers'] > 0:
                    worker_found = True
                    break

            if worker_found:
                l.debug("Found atleast one worker with the task: " + task_name + " registered")
            else:
                l.error("Did not find any workers with the task: " + task_name + " registered")
                sys.exit(1)


            # Gearman client should now submit tasks to the gearman workers
            # We submit jobs based on what is specified in parallelism
            self._gmclient = gmclient = gearman.GearmanClient(gearman_servers)
            num_hosts = len(self._hosts)
            num_parallel = get_num_hosts(self._parallelism, num_hosts)

            if num_parallel == None:
                l.error("The parallelism key should be a positive number")
                sys.exit(1)

            start = 0
            while True:
                for i in range(start, start + num_parallel):
                    try:
                        host = str(self._hosts[i]) # Gearman fails on unicode strings
                        debug_str = "job_id: " + self._job_id + ", command: " + self._command
                        debug_str += ", host: " + host + ", retries: " + str(self._retries)
                        l.debug("Submitting job with the following attributes to the gearman worker: " + debug_str)
                        worker_args = json.dumps({ "host": host, "command": self._command, "retries": str(self._retries) })
                        gmjob = gmclient.submit_job(task_name, worker_args, background=False, wait_until_complete=False)
                        self._gmjobs.append(gmjob)

                    except IndexError:
                        return

                self.poll()
                start = start + i + 1

        except gearman.errors.ServerUnavailable:
            l.error("Gearman server(s): " + str(gearman_servers) + " not available!")
            sys.exit(1)


    def poll(self):
        try:
            self._completed_gmjobs = self._gmclient.wait_until_jobs_completed(self._gmjobs, poll_timeout=self._timeout)

        except gearman.errors.ServerUnavailable:
            l.error("Gearman server(s): " + str(gearman_servers) + " not available!")
            sys.exit(1)

        for index, gmjob in enumerate(self._completed_gmjobs):
            unique = gmjob.job.unique
            if gmjob.state == gearman.job.JOB_COMPLETE:
                self._rcs[unique] = 0
            elif gmjob.state == gearman.job.JOB_FAILED:
                self._rcs[unique] = 1
            elif gmjob.state == gearman.job.JOB_UNKNOWN:
                self._rcs[unique] = 2
            else:
                self._rcs[unique] = 3

            self._output[unique] = gmjob.result


    def success(self):
        # Convert pct values into numbers
        num_hosts = get_num_hosts(self._success_constraint, len(self._hosts))
        if num_hosts == None:
            l.error("The success_constraint should be a positive number")
            sys.exit(1)

        # Check the status codes for each host
        success_count = 0
        for rc in self._rcs.values():
            if rc == 0:
                success_count = success_count + 1

        if success_count >= num_hosts:
            return True
        else:
            return False

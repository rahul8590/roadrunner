#!/usr/bin/env python
import os
import sys

#
# Job class
#
class Job:

    def __init__(self, job_id, timeout, retries, success_constraint
                 parallelism, command, hosts):
        self._job_id  = job_id
        self._timeout = timeout
        self._retries = retries
        self._success_constraint = success_constraint
        self._parallelism = parallelism
        self._command = command
        self._hosts = hosts
        self._rcs = {} # Individual return codes for each host
        self._output = {} # Ouputs per host in the job
        self._success = False

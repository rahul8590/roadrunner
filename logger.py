#!/usr/bin/env python
import logging

#
# Logger class to be used in all modules
#
class Logger:

    def __init__(self, tag):
        self._l = logging.getLogger(tag)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s [' + tag + '] %(message)s')
        handler.setFormatter(formatter)
        self._l.addHandler(handler)
        self._l.setLevel(logging.DEBUG)

    def get(self):
        return self._l

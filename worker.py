import sys , os , simplejson
from fabric import *
from fabric.api import *
import gearman
from gearman import GearmanWorker


#executing the fab command.
#gmJob contains json data  containing dict of host , pass , cmd[list data type]  ( to be executed) 
def exe_job(gmWorker , gmJob ):
 d = simplejson.loads(gmJob.data)
 env.host_string = d['host'] 
 env.password = d['pass']  #will store the password .
 cmds = d['cmd']
 print cmds
 for i in cmds:
  sudo (i )	
 return "job sucessfull"
	
  
#woker node id to be specified in here
gm_worker = gearman.GearmanWorker(['localhost:4730'])
#gm_worker.set_client_id('client1')
gm_worker.register_task('exe_job',exe_job)
gm_worker.work()


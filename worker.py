import sys , os , simplejson
from fabric import *
from fabric.api import *
import gearman
from gearman import GearmanWorker




#executing the fab command . All the configurations are  mentioned in fabfile.py
def exe_job(gmWorker , gmJob ):
 #host = 'synergy.corp.yahoo.com'
 #d = json.loads(gmJob)
 #run (str(d[cmd]), str(d[host]))
 d = simplejson.loads(gmJob.data)
 #print d
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


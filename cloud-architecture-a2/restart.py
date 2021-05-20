import os
import sys
import subprocess
import signal
import time
import timeloop
from timeloop import Timeloop
from datetime import timedelta
tl = Timeloop()
purge_days = 2
def startProcess():
  path="/home/ec2-user/kafka_2.12-2.5.0"

  subprocess.Popen(['nohup','python','Subscribe-cloud.py'])
  #subprocess.Popen(["python3","consumer.py"])
  print("SUBSCRIBE CLOUD restarted")

@tl.job(interval=timedelta(seconds=10))
def sample_job_every_5ms():
    processes = []

    for line in os.popen("ps ax | grep Subscribe-cloud.py"):
        pid = line.split(' ')[0]
        print(line)
        processes.append(pid)

    print(processes,'total number of processes running')


    if len(processes) != 8:
        for proc in processes:
            try:
                os.kill(int(proc), signal.SIGKILL)
                print('KILLING PROCESS',proc)
            except:
                pass

        startProcess()
    else:
        print('NO NEED TO DO ANYTHING, ALREADY RUNNING')

if __name__ == "__main__":
  tl.start(block=True)
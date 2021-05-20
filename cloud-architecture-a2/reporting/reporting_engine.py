from payload.payload import Payload
from multiprocessing import Pool
from sqlops.Postgres import Database
import threading
import sched, time
import json


class Reporting():
    def __init__(self,q):
        self.q = q
        self.payload = []
        f_stop = threading.Event()
        self.f(f_stop)
        self.do_read()

    def f(self,f_stop):
    # do something here ...
        for load in self.payload:
            self.send_reports(load)
        if not f_stop.is_set():
            # call f() again in 10 seconds
            print('running the reporting engine')
            threading.Timer(30, self.f, [f_stop]).start()

    def send_reports(self,payload):
        """
        This takes in a payload object and parses it to run the respective information needed
        """

        database = Database(payload.dict['cameraid'],
                            payload.dict['logId'],
                            payload.dict['human-readable-regex'])

        database.generate_report(payload.commands,
                                 payload.dict['table_name'])


    def do_read(self,):
        while True:
            msg = self.q.get()
            msg = json.loads(msg)
            self.payload.append(Payload(msg,orch=False))

    
    def do_something(self,): 
        print("Hello, World!")
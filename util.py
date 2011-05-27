import time, threading, json, sql
import __main__

def isOdd(num):
    return num & 1 and True or False

def tableSuffix():
    if isOdd(int(time.time())/3600): return "_1"
    return "_2"
            
class status(threading.Thread): #separate thread that periodically pushes status updates to sql
    def __init__(self):
        threading.Thread.__init__(self)
        self.start_time = time.time()
        self.status_data = {}
        self.start()
        
    def run(self):
        while True:
            time.sleep(30)
            t = int(time.time())
            compiled_data = {60:{}, 3600:{}, 86400:{}} #compile data for intervals of a minute, hour and day
            for k in self.status_data.keys():
                for limit in compiled_data.keys():
                     if int(k) >= (t - limit):
                        for event_name in self.status_data[k].keys():
                            if not compiled_data[limit].has_key(event_name): compiled_data[limit][event_name] = 0
                            compiled_data[limit][event_name] += self.status_data[k][event_name]
                if int(k) < (t - 86400): self.status_data.pop(k)            
            compiled_data["start_time"] = self.start_time
            compiled_data["compiled"] = t
            
            sql_data = {"script":__main__.__file__, "added":time.time(), "data":json.dumps(compiled_data)}
            cursor = sql.slytics1().connection.cursor()
            sql.insertRow(cursor, "script_statuses"+tableSuffix(), sql_data, False, True)
            cursor.connection.close()
            cursor.close()
            
    def event(self, event_name):
        t = str(int(time.time()))
        if not self.status_data.has_key(t): self.status_data[t] = {}
        if not self.status_data[t].has_key(event_name): self.status_data[t][event_name] = 0
        self.status_data[t][event_name] +=1
            

import sql, json, time, re, urllib2, threading, Queue
from urlparse import urlparse

lock = threading.Lock()
conn = sql.slytics1().connection
cursor = conn.cursor()
count = 0
q = Queue.Queue()

def needsExpansion(url):
    parsed = urlparse(url).hostname.lower()
    if "youtu.be" in parsed or "youtube.com" in parsed: return False
    return True
    
class worker(threading.Thread):
    def run(self):
        while True:
            lock.acquire()
            if not q.empty():
                jdata = q.get()
                print "queue size: ", q.qsize()
                lock.release()
                status_id = jdata["id"]
                text = ""
                if "_" in list(str(status_id)): #facebook status
                    keys = ["message", "link", "description", "source"]
                    for key in keys:
                        if key in jdata: text += " "+jdata[key]
                else: #twitter status
                    text = jdata["text"]
        
                urls = re.findall("(?P<url>https?://[^\s]+)", text)
                for url in urls:
                        u = url
                        lock.acquire()
                        global count
                        count +=1
                        statusEvent("count")
                        lock.release()
                        if needsExpansion(url) == True: 
                            try:
                                response = urllib2.urlopen(str(url))
                                u = response.url
                                response.close()
                            except:
                                pass
                                print "expansion exception"
                        #print url, " / ", u
            else:
                lock.release()
            time.sleep(0.1)
workers = []
for i in range(40):
    workers.append(worker())
for w in workers:
    w.start()
    
def isOdd(num):
    return num & 1 and True or False

def tableSuffix():
    if isOdd(int(time.time())/3600): return "_1"
    return "_2"
start_time = time.time()
status_data = {}

def statusEvent(event_name):
    t = str(int(time.time()))
    if not status_data.has_key(t): status_data[t] = {}
    if not status_data[t].has_key(event_name): status_data[t][event_name] = 0
    status_data[t][event_name] +=1
            
class status(threading.Thread): #separate thread that periodically pushes status updates to sql
    def run(self):
        while True:
            time.sleep(30)
            t = int(time.time())
            compiled_data = {60:{}, 3600:{}, 86400:{}} #compile data for intervals of a minute, hour and day
            for k in status_data.keys():
                for limit in compiled_data.keys():
                     if int(k) >= (t - limit):
                        for event_name in status_data[k].keys():
                            if not compiled_data[limit].has_key(event_name): compiled_data[limit][event_name] = 0
                            compiled_data[limit][event_name] += status_data[k][event_name]
                if int(k) < (t - 86400): status_data.pop(k)            
            compiled_data["start_time"] = start_time
            compiled_data["compiled"] = t
            compiled_data["queue_count"] = q.qsize()
            sql_data = {"script":__file__, "added":time.time(), "data":json.dumps(compiled_data)}
            sql.insertRow(cursor, "script_statuses"+tableSuffix(), sql_data)
status().start()
    
while True:
    cursor.execute("select data from statuses")
    res = cursor.fetchone()
    ids = ['dummy_value']
    while res:
        jdata = json.loads(res[0])
        status_id = str(jdata["id"])
        ids.append(status_id)
        lock.acquire()
        q.put(jdata)
        lock.release()
        res = cursor.fetchone()
    print len(ids)
    cursor.execute("delete from statuses where id in%s" % sql.formatList(ids))
    cursor.connection.commit()
    time.sleep(1)

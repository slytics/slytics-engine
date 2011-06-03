import sql, httplib, json, time, threading, Queue
from util import *

lock = threading.Lock()
conn = sql.slytics1().connection
cursor = conn.cursor()
status = status()
q = Queue.Queue()

total_counts = {} #used to check whether polled data has changed, and thus how to udpate database; {video_id:total_count}
video_ids_seen = set([]) #more efficient to use set that to check 'if key in dict'

class getIDs(threading.Thread): #constantly populate a list with all unique ids from table youtube_ids
    def __init__(self):
        threading.Thread.__init__(self)
        self.ids = []
        self.ll = self.ul = "1970-01-01 10:10:10" #initial timestamp upper and lower limits
        self.start()
    def run(self):
        while True:
            self.conn = sql.slytics1().connection
            self.cursor = self.conn.cursor()
            self.cursor.execute("select max(timestamp) from youtube_ids")
            self.ul = sql.formatDatetime(self.cursor.fetchone()[0])
            self.conn.commit()
            
            skip = 0
            while skip != -1:
                self.cursor.execute("select id from youtube_ids where timestamp >= '"+self.ll+"' and timestamp < '"+self.ul+"' limit "+str(skip*1000)+", 1000")
                res = self.cursor.fetchone()
                if not res: skip = -2
                skip +=1
                while res:
                    self.ids.append(res[0])
                    status.event("ids_listed")
                    res = self.cursor.fetchone()
                self.conn.commit()
            self.conn.close()
            self.ll = self.ul
            time.sleep(10)
get_ids = getIDs()

start_time = time.time()
class worker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.httpconn = httplib.HTTPSConnection("api.facebook.com")
        self.start()
    def run(self):
        base_url = "/method/links.getStats?format=json&urls="
        youtube_base = "youtube.com%2Fwatch%3Fv%3D"
        while True:
            lock.acquire()
            req_ids = []
            while len(req_ids) < 850 and not q.empty(): req_ids.append(q.get())
            if q.empty(): 
                for ytid in get_ids.ids: q.put(ytid)
            lock.release()
            
            if len(req_ids) > 0:
                req_url = base_url
                for req_id in req_ids: req_url += youtube_base + req_id + ","
                self.httpconn.request("GET", req_url)
                res = self.httpconn.getresponse()
                status.event("requests")
                retrieved = time.time()
                jdata = {}
                parsed = json.loads(res.read())
                lock.acquire()
                error_thrown = False
                for video in parsed:
                    try:
                        table_suffix = tableSuffix() #call once to ensure consistency across both times used in this method
                        video_id = getVideoID(video["normalized_url"])
                        sql_data = {"video_id":video_id, "data":""}
                        sql.insertRow(cursor, "facebook_polldata"+table_suffix+", sql_data", True)
                        total_count = video["total_count"]
                        if video_id in video_ids_seen and total_counts[video_id]==total_count:
                            status.event("data_unchanged") #data has remained unchanged since last request for this video
                        else:
                            video_ids_seen.add(video_id)
                            total_counts.update({video_id:total_count})
                            jdata = video
                            status.event("data_changed")
                        status.event("urls_polled")
                        jdata.update({"retrieved":retrieved})
                        cursor.execute("update facebook_polldata"+table_suffix+" set data = concat(data, '"+sql.fixS(json.dumps(jdata))+"') where video_id = '"+sql.fixS(video_id)+"'")
                    except:
                        error_thrown = True
                if error_thrown==True:
                    for req_id in req_ids: q.put(req_id)
                    status.event("request_errors")
                conn.commit()
                lock.release()
                
workers = []
for i in range(50): workers.append(worker()) #fire up worker threads    

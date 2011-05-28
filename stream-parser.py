import sql, json, time, re, urllib2, threading, Queue, urlparse, httplib
from util import *

lock = threading.Lock()
conn = sql.slytics1().connection
cursor = conn.cursor()
status = status()
q = Queue.Queue()

def needsExpansion(url):
    parsed = urlparse.urlparse(url).hostname
    if parsed==None: return False
    if "youtu.be" in parsed or "youtube.com" in parsed: return False
    return True
    
def expandURL(url):
    parsed = urlparse.urlparse(url)
    h = httplib.HTTPConnection(parsed.netloc)
    h.request('HEAD', str(parsed.path.encode("utf-8")))
    response = h.getresponse()
    if response.status/100 == 3 and response.getheader('Location'):
        return response.getheader('Location')
    else:
        return url

def getVideoID(url):
    parsed = urlparse.urlparse(url)
    host = parsed.hostname
    res = None
    if "youtu.be" in host: res = parsed.path.replace("/", "")
    if "youtube.com" in host:
        if parsed.path=="/watch": 
            query = urlparse.parse_qs(parsed.query)
            if query.has_key("v"): res = query["v"][0]
        if "/v/" in parsed.path: res= parsed.path.replace("/v/", "")
        if "/embed/" in parsed.path: res= parsed.path.replace("/embed/", "")
    if not res==None:
        if len(res) >= 11: return res.strip()[:11]
    
class worker(threading.Thread):
    def run(self):
        while True:
            lock.acquire()
            if not q.empty():
                jdata = q.get()
                lock.release()
                status_id = jdata["id"]
                text = ""
                status_type = "facebook"
                if "_" in list(str(status_id)): #facebook status
                    keys = ["message", "link", "description", "source"]
                    for key in keys:
                        if key in jdata: text += " "+jdata[key]
                    status.event(status_type+"_statuses_parsed")
                else: #twitter status
                    text = jdata["text"]
                    status_type = "twitter"
                    status.event(status_type+"_statuses_parsed")
                urls = re.findall("(?P<url>https?://[^\s]+)", text)
                videos = []
                for url in urls:
                    u = url
                    if needsExpansion(url)==True: 
                        u = expandURL(url)
                    video_id = getVideoID(u)
                    if video_id!=None and not video_id in videos: videos.append(video_id)
                    if video_id!=None:
                        lock.acquire()
                        sql_data = {"original_url":url[:200], "expanded_url":u[:1000], "video_id":video_id}
                        sql.insertRow(cursor, "youtube_urls", sql_data, True)
                        cursor.connection.commit()
                        lock.release()
                    status.event(status_type+"_urls_found")
                lock.acquire()
                for video in videos:
                    sql_data = {"id":video}
                    sql.insertRow(cursor, "youtube_ids", sql_data, True)
                    status.event(status_type+"_videos_found") 
                cursor.connection.commit()
                lock.release()
            else:
                lock.release()
            time.sleep(0.1)

#fire up worker threads            
workers = []
for i in range(40):
    workers.append(worker())
for w in workers:
    w.start()

#continuously populate up the queue
queue_conn = sql.slytics1().connection
queue_cursor = queue_conn.cursor()
service = "twitter"
max_id = {"twitter":0, "facebook":0}
table_suffix = {"twitter":tableSuffix(), "facebook":tableSuffix()}
while True:
    queue_cursor.execute("select data, sid, id from "+service+"_statuses"+table_suffix[service]+" where sid > "+str(max_id[service])+" limit 500")
    res = queue_cursor.fetchone()
    if res==None:
        if table_suffix[service] != tableSuffix():
            queue_cursor.execute("truncate table "+service+"_statuses"+table_suffix[service])
            table_suffix[service] = tableSuffix()
            max_id[service] = 0
    while res:
        lock.acquire()
        q.put(json.loads(res[0]))
        lock.release()
        max_id[service] = res[1]
        res = queue_cursor.fetchone()
    queue_cursor.connection.commit()
    if service=="twitter":
        service = "facebook"
    else:
        service = "twitter"
    time.sleep(0.1)

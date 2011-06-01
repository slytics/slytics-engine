import sql, json, time, re, urllib2, threading, Queue, urlparse, httplib
from util import *

lock = threading.Lock()
conn = sql.slytics1().connection
cursor = conn.cursor()
status = status()
q = Queue.Queue()

class worker(threading.Thread):
    def run(self):
        while True:
            lock.acquire()
            if not q.empty():
                jdata = q.get()
                lock.release()
                status_id = jdata["id"]
                text = ""
                for key in ["message", "link", "description", "source"]:
                    if key in jdata: text += " "+jdata[key]
                status.event("statuses_parsed")
                urls = re.findall("(?P<url>https?://[^\s]+)", text)
                videos = []
                for url in urls:
                    video_id = getVideoID(url)                   
                    if video_id!=None and not video_id in videos: videos.append(video_id)
                    if video_id!=None:
                        lock.acquire()
                        sql_data = {"original_url":url[:200], "expanded_url":url[:1000], "video_id":video_id}
                        sql.insertRow(cursor, "youtube_urls", sql_data, True)
                        cursor.connection.commit()
                        lock.release()                                                                   
                    status.event("urls_found")
                lock.acquire()
                for video in videos:
                    sql_data = {"id":video}
                    sql.insertRow(cursor, "youtube_ids", sql_data, True)
                    status.event("videos_found") 
                cursor.connection.commit()
                lock.release()
            else:
                lock.release()
            time.sleep(0.1)

#fire up worker threads            
workers = []
for i in range(50):
    workers.append(worker())
for w in workers:
    w.start()

#continuously populate up the queue
queue_conn = sql.slytics1().connection
queue_cursor = queue_conn.cursor()
max_id = 0
table_suffix = tableSuffix()
while True:
    queue_cursor.execute("select data, sid, id from facebook_statuses"+table_suffix+" where sid > "+str(max_id)+" limit 500")
    res = queue_cursor.fetchone()
    if res==None:
        if table_suffix != tableSuffix():
            queue_cursor.execute("truncate table facebook_statuses"+table_suffix)
            table_suffix = tableSuffix()
            max_id = 0
    while res:
        lock.acquire()
        q.put(json.loads(res[0]))
        lock.release()
        max_id = res[1]
        res = queue_cursor.fetchone()
    queue_cursor.connection.commit()
    time.sleep(0.1)

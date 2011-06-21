import sql, json, time, re, urllib2, threading, Queue, urlparse, httplib, operator
from util import *

lock = threading.Lock()
conn = sql.slytics1().connection
cursor = conn.cursor()
status = status()
q = Queue.Queue()
stat_data = []


class worker(threading.Thread):
    def run(self):
        while True:
            lock.acquire()
            if not q.empty():
                jdata = q.get()
                lock.release()
                status_id = jdata["id"]
                user_id = status_id.split("_")[0]
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
                    stat_data.append([user_id, video, time.time()]) 
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
    
#fire up stat counting thread
#to do: upon startup, grab extant data from last time stats were counted (if any); delete data > 24 hours old
class counter(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.start()
    def run(self):
        while True:
            time.sleep(30)
            videos = set([])
            users = set([])
            post_counts = {}
            jdata = {}
            jdata["total_posts"] = len(stat_data)
            total_posts = len(stat_data)
            for i in range(len(stat_data)):
                user_id = stat_data[i][0]
                video_id = stat_data[i][1]
                status_time = stat_data[i][2]
                if status_time < (time.time() - 86400):
                    stat_data.pop(i)
                else:
                    videos.add(video_id)
                    users.add(user_id)
                    if not video_id in post_counts.keys(): post_counts[video_id] = 0
                    post_counts[video_id] +=1
            jdata["unique_users"] = len(users)
            jdata["unique_videos"] = len(videos)     
            sorted_counts = sorted(post_counts.iteritems(), key=operator.itemgetter(1))
            sorted_counts.reverse()
            jdata["top_100"] = sorted_counts[0:100]
            lock.acquire()
            sql_data = {"type":"facebook_posts", "added":str(time.time()), "data":json.dumps(jdata)}
            sql.insertRow(cursor, "parsed_data"+tableSuffix(), sql_data, True)
            cursor.connection.commit()
            lock.release()  
            
stat_counter = counter()             
             
#continuously populate up the queue
queue_conn = sql.slytics1().connection
queue_cursor = queue_conn.cursor()
max_id = 0
table_suffix = tableSuffix()
while True:
    queue_cursor.execute("select data, sid, id from facebook_statuses"+table_suffix+" where sid > "+str(max_id)+" limit 500")
    res = queue_cursor.fetchone()
    if res==None and table_suffix != tableSuffix():
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

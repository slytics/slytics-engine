import httplib, socket, time, json, MySQLdb, sql, threading
cursor = sql.slytics1().connection.cursor()
lock = threading.Lock()

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
            
            sql_data = {"script":__file__, "added":time.time(), "data":json.dumps(compiled_data)}
            lock.acquire()
            sql.insertRow(cursor, "script_statuses"+tableSuffix(), sql_data)
            cursor.connection.commit()
            lock.release()
status().start()

conn = httplib.HTTPConnection("search.twitter.com")
since_id = 0
while True:
    resp = "{}"
    try:
        conn.request("GET", "/search.json?q=youtube.com&rpp=100&result_type=recent&filter=links&since_id="+str(since_id), None, {"User-Agent":"VideoMuffin"})
        statusEvent("requests")
        res = conn.getresponse()
        if str(res.status) !=  "200": statusEvent("non_200_responses")
        resp = res.read()
    except socket.error as ex:
        print ex
    
    try:
        parsed = json.loads(resp)
    except ValueError:
        print "Twitter returned a malformed JSON object"
        
    tweets_this_request = 0

    if parsed.has_key("results"): 
        for tweet in parsed["results"]:
            statusEvent("tweets")
            tweets_this_request +=1
            if since_id < tweet["id"]: since_id = tweet["id"]
            sql_data = {"id":tweet["id"], "data":json.dumps(tweet)}
            
            lock.acquire()
            sql.insertRow(cursor, "twitter_statuses"+tableSuffix(), sql_data, True)
            sql.insertRow(cursor, "statuses", sql_data, True)
            lock.release()
    if tweets_this_request == 100: statusEvent("pegged_requests")
    
    sql_data = {"time":str(time.time()), "since_id":str(since_id), "status_code":str(res.status), "results":str(tweets_this_request)} 
    lock.acquire()
    sql.insertRow(cursor, "twitter_requests"+tableSuffix(), sql_data)
    cursor.connection.commit()   
    lock.release()
    
    conn.close()
    time.sleep(1)

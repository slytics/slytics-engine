import httplib, socket, time, json, MySQLdb, sql
from util import *

cursor = sql.slytics1().connection.cursor()
status = status()
conn = httplib.HTTPConnection("search.twitter.com")

since_id = 0
while True:
    resp = "{}"
    try:
        conn.request("GET", "/search.json?q=youtube.com&rpp=100&result_type=recent&filter=links&since_id="+str(since_id), None, {"User-Agent":"VideoMuffin"})
        status.event("requests")
        res = conn.getresponse()
        if str(res.status) !=  "200": status.event("non_200_responses")
        resp = res.read()
    except socket.error as ex:
        status.event(str(ex))
    except httplib.IncompleteRead:
        status.event("incomplete_read")
    
    try:
        parsed = json.loads(resp)
    except ValueError:
        parsed = {}
        status.event("json_value_errors")
        
    if parsed.has_key("results"):
        tweets_this_request = 0 
        for tweet in parsed["results"]:
            tweets_this_request +=1
            status.event("tweets")
            if since_id < tweet["id"]: since_id = tweet["id"]
            sql_data = {"id":tweet["id"], "data":json.dumps(tweet)}
            sql.insertRow(cursor, "twitter_statuses"+tableSuffix(), sql_data, True)
        if tweets_this_request == 100: status.event("pegged_requests")
    sql_data = {"time":str(time.time()), "since_id":str(since_id), "status_code":str(res.status), "results":str(tweets_this_request)} 
    sql.insertRow(cursor, "twitter_requests"+tableSuffix(), sql_data)
    cursor.connection.commit()   
    conn.close()
    time.sleep(1)

import httplib, time, json, MySQLdb, sql

started = time.time()
requests = 0
tweets = 0
since_id = 0

while True:
    conn = httplib.HTTPConnection("search.twitter.com")
    try: 
        conn.request("GET", "/search.json?q=youtube.com&rpp=100&result_type=recent&filter=links&since_id="+str(since_id), None, {"User-Agent":"VideoMuffin"})
        res = conn.getresponse()
    except socket.error, (value,message):
        print "Socket error %s: %s" % (value, message)
    requests +=1
    
    try:
        parsed = json.loads(res.read())
    except ValueError:
        print "Twitter returned a malformed JSON object"
        
    tweets_this_request = 0
    if parsed.has_key("results"): 
        cursor = sql.slytics1.cursor()
        for tweet in parsed["results"]:
            tweets +=1
            tweets_this_request +=1
            if since_id < tweet["id"]: since_id = tweet["id"]
            sql_data = {"id":tweet["id"], "data":json.dumps(tweet)}
            sql.insertRow(cursor, "twitter_statuses", sql_data, True)
        cursor.close()
    conn.close()
    
    #print "\n ", res.status, res.reason
    #print "  %s requests in the past %s seconds, %s tweets found, since_id = %s" % (requests, time.time()-started, tweets, since_id)
    #print "  %s tweets this request, average %s tweets/second" % (tweets_this_request, tweets/(time.time()-started))
    time.sleep(.5)

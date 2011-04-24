import httplib, time, json, MySQLdb
since_id = 0
started = time.time()
requests = 0
tweets = 0

db=MySQLdb.connect(host="slytics1",user="root",passwd="clt052$",port=int(3306),db="slytics")
cursor=db.cursor()

while True:
    conn = httplib.HTTPConnection("search.twitter.com")
    conn.request("GET", "/search.json?q=youtube.com&rpp=100&result_type=recent&since_id=" + str(since_id+1))
    res = conn.getresponse()
    requests +=1
    
    try:
        parsed = json.loads(res.read())
    except ValueError:
        print "Twitter returned a malformed JSON object"
        
    tweets_this_request = 0
    if parsed.has_key("results"): 
        for tweet in parsed["results"]:
            if tweet["id"] > since_id:
                since_id = tweet["id"]
            tweets +=1
            tweets_this_request +=1
            
            try:
                cursor.execute("insert into twitter_statuses(id, data) values(%s, %s)" % (tweet["id"], "'"+str.replace(json.dumps(tweet),"'","''")+"'"))
            except MySQLdb.IntegrityError, message:
                if message[0] == 1062: 
                    print "MySQL duplicate key exception"
                else:
                    print message
                    raise MySQLdb.IntegrityError
    else:
        if parsed.has_key("error"):
            print parsed["error"]
            if parsed["error"] == "since_id too recent, poll less frequently":
                time.sleep(1)
            else:
                 print json.dumps(parsed)
                 raise error
        else:
            print json.dumps(parsed)
            raise error
            
    conn.close()
    print "\n ", res.status, res.reason
    print "  %s requests in the past %s seconds, %s tweets found" % (requests, time.time()-started, tweets)
    print "  %s tweets in this request, since_id = %s" % (tweets_this_request, since_id)
    print "  average %s tweets/second" % (tweets/(time.time()-started))
    time.sleep(1)

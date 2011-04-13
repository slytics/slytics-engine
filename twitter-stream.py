import httplib, time, json
since_id = 0
started = time.time()
requests = 0
tweets = 0

while True:
    conn = httplib.HTTPConnection("search.twitter.com")
    conn.request("GET", "/search.json?q=youtube&rpp=100&result_type=recent&since_id=" + str(since_id+1))
    res = conn.getresponse()
    requests +=1
    
    try:
        parsed = json.loads(res.read())
    except:
        print res.read
        
    tweets_this_request = 0
    if parsed.has_key("results"): 
        for tweet in parsed["results"]:
            if tweet["id"] > since_id:
                since_id = tweet["id"]
            print tweet["text"]
            tweets +=1
            tweets_this_request +=1
            
    conn.close()
    print "\n ", res.status, res.reason
    print "  %s requests in the past %s seconds, %s tweets found" % (requests, time.time()-started, tweets)
    print "  %s tweets in this request, since_id = %s" % (tweets_this_request, since_id)
    time.sleep(1)

import httplib, time, json, MySQLdb, threading

search_terms = ["youtube.com","soundcloud.com","flickr.com","vimeo.com","huffingtonpost.com","nytimes.com","techcrunch.com","gawker.com","twitpic.com","one","two","three","four","five","six","seven","eight","nine","ten","eleven","twelve","thirteen","fourteen","fifteen","sixteen"]
db = MySQLdb.connect(host="slytics1",user="root",passwd="clt052$",port=int(3306),db="slytics")
cursor = db.cursor()
lock = threading.Lock()
started = time.time()
requests = 0
tweets = 0

class requester(threading.Thread):
    def __init__(self, searchTerm):
        threading.Thread.__init__(self)
        self.search_term = searchTerm
    def run(self):
        while True:
            lock.acquire()
            conn = httplib.HTTPConnection("search.twitter.com")
            conn.request("GET", "/search.json?q="+self.search_term+"&rpp=100&result_type=recent") #excized '&since_id=" + str(since_id+1)' from request
            res = conn.getresponse()
            global requests
            requests +=1
    
            try:
                parsed = json.loads(res.read())
            except ValueError:
                print "Twitter returned a malformed JSON object"
        
            tweets_this_request = 0
            if parsed.has_key("results"): 
                for tweet in parsed["results"]:
                    global tweets
                    tweets +=1
                    tweets_this_request +=1
           
            conn.close()
            print "\n ", res.status, res.reason, self.search_term
            print "  %s requests in the past %s seconds, %s tweets found" % (requests, time.time()-started, tweets)
            print "  average %s tweets/second" % (tweets/(time.time()-started))
            lock.release()

for term in search_terms:
    requester(term).start()


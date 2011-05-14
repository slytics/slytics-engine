import httplib, socket, time, json, MySQLdb, sql, xml.etree.ElementTree, urllib, urlparse, threading
cursor = sql.slytics1().connection.cursor()
access_token = "189798971066603|486f5fac1bb43befc78b7e14.1-1357950042|LOONKF6Zp8yVXff-Ck5i1sC2hk0"
lock = threading.Lock()

def isOdd(num):
    return num & 1 and True or False

def tableSuffix():
    if isOdd(int(time.time())/3600): return "_1"
    return "_2"
    
def getLocales():
    """returns list of all supported facebook locales found at facebook.com/translations/FacebookLocales.xml"""
    conn = httplib.HTTPConnection("www.facebook.com")
    conn.request("GET", "/translations/FacebookLocales.xml")
    
    tree = xml.etree.ElementTree.fromstring(conn.getresponse().read())
    conn.close()
    
    res = []    
    locales = tree.findall("locale")
    for locale in locales:
        representation = locale.find("codes").find("code").find("standard").find("representation").text
        if representation != "tl_ST" and representation != "ck_US": res.append(representation)
        #it's a little bizarre, but the above two locales appear to return search results for _all_ locales
    return res
        
locales_list = getLocales()
locales = {}
status_data = {}
for locale in locales_list:
    locales[locale] = {"since":int(time.time()), "skip":{}, "last_retrieve":time.time(), "next_retrieve":time.time()}
    status_data[locale] = {}

start_time = time.time()
def statusEvent(locale, event_name):
    t = str(int(time.time()))
    if not status_data[locale].has_key(t): status_data[locale][t] = {}
    if not status_data[locale][t].has_key(event_name): status_data[locale][t][event_name] = 0
    status_data[locale][t][event_name] +=1
    
class status(threading.Thread): #separate thread that periodically pushes status updates to sql
    def run(self):
        while True:
            time.sleep(30)
            t = int(time.time())
            compiled_data = {60:{}, 3600:{}, 86400:{}} #compile data for intervals of a minute, hour and day
            for l in status_data.keys():
                for limit in compiled_data.keys():
                    compiled_data[limit][l] = {}
                for k in status_data[l].keys():
                    for limit in compiled_data.keys():
                        if int(k) >= (t - limit):
                            for event_name in status_data[l][k].keys():
                                if not compiled_data[limit][l].has_key(event_name): compiled_data[limit][l][event_name] = 0
                                compiled_data[limit][l][event_name] += status_data[l][k][event_name]
                    if int(k) < (t - 86400): status_data[l].pop(k)
                
            compiled_data["start_time"] = start_time
            compiled_data["compiled"] = t
            
            sql_data = {"script":__file__, "added":time.time(), "data":json.dumps(compiled_data)}
            lock.acquire()
            sql.insertRow(cursor, "script_statuses"+tableSuffix(), sql_data)
            cursor.connection.commit()
            lock.release()
status().start()
    
conn = httplib.HTTPSConnection("graph.facebook.com")
while True:
    for l in locales:
        locale = locales[l]
        if locale["next_retrieve"] <= int(time.time()):
            
            parsed = {}
            try:
                conn.request("GET", "/search?q=http://&type=post&limit=500&locale=%s&since=%s&access_token=%s" % (l, locale["since"], access_token))
                statusEvent(l, "requests")
                res = conn.getresponse() #this is where a socket error occurred; need to be mindful of this for twitter-stream as well
                if str(res.status) !=  "200": statusEvent(l, "non_200_responses")
                parsed = json.loads(res.read())
            except socket.error as ex:
                print ex

            if parsed.has_key("data"):
                if parsed.has_key("paging"): locales[l]["since"] = int(urlparse.parse_qs(urlparse.urlparse(parsed["paging"]["previous"])[4])["since"][0]) - 1
                delay = ( 100 / ( (len(parsed["data"]) + 5) / (time.time() - locale["last_retrieve"]) ) )
                if delay > 300: delay = 300
                locales[l]["next_retrieve"] = time.time() + delay
                locales[l]["last_retrieve"] = time.time()
            
                for post in parsed["data"]:
                    statusEvent(l, "posts")
                    post["from"]["locale"] = l
                    sql_data = {"id":post["id"], "data":json.dumps(post)}
                    lock.acquire()
                    if not locales[l]["skip"].has_key(post["id"]): sql.insertRow(cursor, "facebook_statuses"+tableSuffix(), sql_data, True)
                    lock.release()
                    if post["updated_time"] == parsed["data"][0]["updated_time"]: locales[l]["skip"][post["id"]] = locale["since"]
        
                for key in locales[l]["skip"].keys():
                    if locales[l]["skip"][key] != locales[l]["since"]: locales[l]["skip"].pop(key)
                
                if len(parsed["data"]) > 480: statusEvent(l, "pegged_requests")
            
            sql_data = {"time":str(time.time()), "locale":l, "since":locale["since"], "status_code":str(res.status), "results":str(len(parsed["data"]))} 
            lock.acquire()
            sql.insertRow(cursor, "facebook_requests"+tableSuffix(), sql_data)
            cursor.connection.commit()
            lock.release()

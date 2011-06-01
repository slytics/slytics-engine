import httplib, socket, time, json, MySQLdb, sql, xml.etree.ElementTree, urllib, urlparse, threading
from util import *

cursor = sql.slytics1().connection.cursor()
status = status()
access_token = "189798971066603|486f5fac1bb43befc78b7e14.1-1357950042|LOONKF6Zp8yVXff-Ck5i1sC2hk0"

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
        
locales_list = getLocales() #a straight-up list of all the locales
locales = {} #a dict that will store, for each locale, time since last retrieval, time until next retrieval, records to skip, etc.
for locale in locales_list:
    locales[locale] = {"since":int(time.time()), "skip":{}, "last_retrieve":time.time(), "next_retrieve":time.time()}

conn = httplib.HTTPSConnection("graph.facebook.com")
while True:
    for l in locales: #l is the name of the locale
        locale = locales[l] #the dict record for this locale
        if locale["next_retrieve"] <= int(time.time()):
            parsed = {}
            try:
                conn.request("GET", "/search?q=http://&type=post&limit=500&locale=%s&since=%s&access_token=%s" % (l, locale["since"], access_token))
                status.event(l+"requests")
                res = conn.getresponse()
                if str(res.status) !=  "200": status.event(l+"non_200_responses")
                parsed = json.loads(res.read())
            except:
                conn = httplib.HTTPSConnection("graph.facebook.com")
            
            if parsed.has_key("data"):
                if parsed.has_key("paging"): locales[l]["since"] = int(urlparse.parse_qs(urlparse.urlparse(parsed["paging"]["previous"])[4])["since"][0]) - 1
                delay = ( 100 / ( (len(parsed["data"]) + 5) / (time.time() - locale["last_retrieve"]) ) )
                if delay > 300: delay = 300
                if l == "en_US": delay = 10
                
                locales[l]["next_retrieve"] = time.time() + delay
                locales[l]["last_retrieve"] = time.time()
            
                for post in parsed["data"]:
                    status.event(l+"posts")
                    if "from" in post: post["from"].update({"locale":l})
                    sql_data = {"id":post["id"], "data":json.dumps(post)}
                    if not locales[l]["skip"].has_key(post["id"]): 
                        sql.insertRow(cursor, "facebook_statuses"+tableSuffix(), sql_data, True)
                    if post["updated_time"] == parsed["data"][0]["updated_time"]: locales[l]["skip"][post["id"]] = locale["since"]
        
                for key in locales[l]["skip"].keys():
                    if locales[l]["skip"][key] != locales[l]["since"]: locales[l]["skip"].pop(key)
                
                if len(parsed["data"]) > 480: status.event(l+"pegged_requests")
            
            result_count = 0
            if parsed.has_key("data"): result_count = len(parsed["data"])
            sql_data = {"time":str(time.time()), "locale":l, "since":locale["since"], "status_code":str(res.status), "results":str(result_count)} 
            sql.insertRow(cursor, "facebook_requests"+tableSuffix(), sql_data)
            cursor.connection.commit()

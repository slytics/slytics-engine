import sql, json, time, re, urllib2, threading, Queue, urlparse, httplib, operator
from util import * #same imports as facebook-parser....they are not all necessary.  prune later

status = status()

conn = sql.slytics1().connection
cursor = conn.cursor()

c = sql.slytics1().connection
ccursor = c.cursor()

max_id = 0
table_suffix = tableSuffix()
while True:
    cursor.execute("select id, data from facebook_polldata"+table_suffix+" where id > "+str(max_id)+" limit 500")
    res = cursor.fetchone()
    if res==None and table_suffix != tableSuffix():
            table_suffix = tableSuffix()
            max_id = 0
    while res:
        max_id = res[0]
        data = json.loads(res[1])
        status.event("rows_processed")
        if "normalized_url" in data.keys():
            vid = getVideoID(data["normalized_url"])
            extant_data = sql.scalar(ccursor, "facebook_pollcount", "data", "video", vid)
            str_data = str(data["retrieved"])+" "+str(data["like_count"])+" "+str(data["share_count"])+" "
            if extant_data==None:
                sql_data = {"video":vid, "data":str_data}
                sql.insertRow(ccursor, "facebook_pollcount", sql_data)
            else:
                ccursor.execute("update facebook_pollcount set data = concat(data, '"+str_data+"') where video = '"+vid+"'")
        res = cursor.fetchone()
    cursor.connection.commit()
    ccursor.connection.commit()
    time.sleep(0.1)

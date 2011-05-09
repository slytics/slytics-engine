import MySQLdb

#connections
slytics1 = MySQLdb.connect(host="slytics1",user="root",passwd="clt052$",port=int(3306),db="slytics")

def formatList(list_object):
    res = "("
    for char in str(list_object)[1:-1]:
        res += char
    return res + ")"
    
def insertRow(cursor, table, data):
    keys = str(formatList(data.keys()).replace("'",""))
    for key in data:
        data[key] = cursor.connection.literal(data[key])
    values = str(formatList(data.values()))
    cursor.execute("insert into  %s%s values%s" % (table, keys, values))

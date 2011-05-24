import MySQLdb

#connections
class slytics1:
    def __init__(self):
        self.connection = MySQLdb.connect(host="slytics1",user="root",passwd="clt052$",port=int(3306),db="slytics")

def formatList(list_object):
    res = "("
    for char in str(list_object)[1:-1]:
        res += char
    return res + ")"
    
def fixS(string):
    return str(string).replace("'","''")
    
def insertRow(cursor, table, data, catch_duplicate = False, auto_commit = False):
    keys = str(formatList(data.keys()).replace("'",""))
    for key in data:
        data[key] = fixS(data[key])
    values = str(formatList(data.values()))
    try:
        cursor.execute("insert into  %s%s values%s" % (table, keys, values))
        if auto_commit == True: cursor.connection.commit()
    except MySQLdb.IntegrityError, message:
        if str(message[0]) == "1062" and catch_duplicate == True: #1062 is duplicate key error code
            pass
        else:
            raise
        

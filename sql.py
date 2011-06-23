import MySQLdb
from time import strftime

class slytics1:
    def __init__(self):
        self.connection = MySQLdb.connect(host="slytics1",user="root",passwd="clt052$",port=int(3306),db="slytics")

def formatList(list_object):
    res = "("
    for char in str(list_object)[1:-1]:
        res += char
    return res + ")"
    
def fixS(string):
    try:
        s = str(string)
    except UnicodeEncodeError as ex:
        s = str(string.encode('utf-8'))
    return s.replace("'","''")
    
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

def scalar(cursor, table, data, primary_key, primary_key_value):
    cursor.execute("select "+data+" from "+table+" where "+primary_key+" = '"+fixS(primary_key_value)+"'")
    res = cursor.fetchone()
    if res==None: return None
    return res[0]
    

def formatDatetime(datetime_object):
    return datetime_object.strftime('%Y-%m-%d %H:%M:%S')

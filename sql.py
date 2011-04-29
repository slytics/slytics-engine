import MySQLdb

def insertRow(table, data):
    conn = MySQLdb.connect(host="slytics1",user="root",passwd="clt052$",port=int(3306),db="slytics")
    cursor = conn.cursor()
    
    for key in data.keys():
        data[key] = conn.literal(data[key])
    
    columns = str(data.keys()).replace("[","(").replace("]",")").replace("'","") #these values are no good because they will replace the actual content of the fields as well
    values = str(data.values()).replace("[","(").replace("]",")") #use mid function instead to alter only first and last char of dict str representation
    
    cursor.execute("insert into  %s%s values%s" % (table, columns, values))
    
    conn.close

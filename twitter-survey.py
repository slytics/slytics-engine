import time, json, tweepy, sql, MySQLdb
from textwrap import TextWrapper

global conn
#conn = MySQLdb.connect(host="slytics1",user="root",passwd="clt052$",port=int(3306),db="slytics")

class StreamWatcherListener(tweepy.StreamListener):

    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')
   
    def on_data(self, data):
        global conn
        try:
            my_dic = json.loads(data)
            if type(my_dic).__name__=='dict':
                if "id" in my_dic:
                    #sql.insertRow("twitter_statuses", {"data":data,"id":my_dic["id"]}, conn)
                    print data
        except ValueError:
            pass
    
    def on_status(self, status):
        pass

    def on_error(self, status_code):
        print 'An error has occured! Status code = %s' % status_code
        return True  # keep stream alive

    def on_timeout(self):
        print 'Snoozing Zzzzzz'


def main():
    auth = tweepy.BasicAuthHandler("danhak", "alphadot25")
    stream = tweepy.Stream(auth, StreamWatcherListener(), timeout=None)
    stream.sample()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print '\nGoodbye!'


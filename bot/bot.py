import sqlite3
import time
import praw
import prawcore
import logging
import datetime
import os
import re
import schedule
import pymysql

os.environ['TZ'] = 'UTC'

con = pymysql.connect(
    host=os.environ['MYSQL_HOST'],
    user=os.environ['MYSQL_USER'],
    passwd=os.environ['MYSQL_PASS'],
    db=os.environ['MYSQL_DB']
)

REDDIT_CID=os.environ['REDDIT_CID']
REDDIT_SECRET=os.environ['REDDIT_SECRET']
REDDIT_USER = os.environ['REDDIT_USER']
REDDIT_PASS = os.environ['REDDIT_PASS']
REDDIT_SUBREDDIT= os.environ['REDDIT_SUBREDDIT']
AGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"

reddit = praw.Reddit(client_id=REDDIT_CID,
                     client_secret=REDDIT_SECRET,
                     password=REDDIT_PASS,
                     user_agent=AGENT,
                     username=REDDIT_USER)
subreddit = reddit.subreddit(REDDIT_SUBREDDIT)

apppath='/storage/'



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
logging.getLogger('schedule').propagate = False




logging.info("starting scheduler...")


def runjob():
  tm = str(int(time.time()))
  cursorObj = con.cursor()
  cursorObj.execute('SELECT * FROM schedules WHERE schedtime <= %s ORDER BY schedtime DESC LIMIT 0,50;', (tm,))
  rows = cursorObj.fetchall()
  if len(rows) is not 0:
    for row in rows:
      submission = reddit.submission(row[1])
      #logging.info( submission.removed_by_category )
      if submission.removed_by_category is None and submission.author is not None and submission.banned_by is None:
       if submission.link_flair_text is None or ("preorder" not in submission.link_flair_text.lower() and "pre-order" not in submission.link_flair_text.lower() and "preorder" not in submission.title.lower() and "pre-order" not in submission.title.lower()):
        if not submission.spoiler:
               #if "expired" not in submission.link_flair_text.lower():
            logging.info("running schedule on https://reddit.com/" + row[1])
            submission.mod.spoiler()
            flairtime = str( int(time.time()))
            cursorObj = con.cursor()
            cursorObj.execute('DELETE FROM schedules WHERE postid = %s', (row[1],))
            cursorObj.execute('INSERT INTO flairs(postid, flairtext, timeset) VALUES(%s,%s,%s)', (submission.id,submission.link_flair_text,flairtime)  )
            con.commit()
            submission.mod.flair(text='Expired', css_class='expired')
       else:
        cursorObj = con.cursor()
        cursorObj.execute('DELETE FROM schedules WHERE postid = %s', (row[1],))
        con.commit()
        logging.info("skipping https://reddit.com/" + row[1])
      else:
        cursorObj = con.cursor()
        cursorObj.execute('DELETE FROM schedules WHERE postid = %s', (row[1],))
        con.commit()
        logging.info("skipping https://reddit.com/" + row[1])





schedule.every(1).minutes.do(runjob)

runjob()
while 1:
    schedule.run_pending()
    time.sleep(30)

import json
import os.path
import sqlite3
from time import mktime, strptime

tcreation = """
CREATE TABLE tweets(
    time INTEGER,
    rtcount INTEGER,
    uid INTEGER,
    tid INTEGER PRIMARY KEY,
    text TEXT,
    geo TEXT
);
CREATE TABLE retweets(
    rtime INTEGER,
    ruid INTEGER,
    rtid INTEGER PRIMARY KEY,
    rtext TEXT,
    rgeo TEXT,

    otime INTEGER,
    ouid INTEGER,
    otid INTEGER,
    otext TEXT,
    ogeo TEXT
);
CREATE TABLE users(
    uid INTEGER PRIMARY KEY,
    screen_name TEXT UNIQUE
);
CREATE TABLE followers(
    src_uid INTEGER,
    dst_uid INTEGER,
    PRIMARY KEY(src_uid, dst_uid)
);
"""

dcreation = """
CREATE TABLE pages(
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    type INTEGER,
    uid INTEGER,
    tid INTEGER UNIQUE,
    json TEXT
);
"""

class Collector(object):
    DUMPS_DB = 'db/dumps.db'
    TWEETS_DB = 'db/tweets.db'

    def __init__(self):
        missing_dumps = (os.path.exists(Collector.DUMPS_DB) == False)
        missing_tweets = (os.path.exists(Collector.TWEETS_DB) == False)

        self.dumps = sqlite3.connect(Collector.DUMPS_DB)
        self.tweets = sqlite3.connect(Collector.TWEETS_DB)

        self.dcursor, self.tcursor = self.dumps.cursor(), self.tweets.cursor()

        if missing_dumps:
            global dcreation
            self._execute(self.dcursor, dcreation)
        if missing_tweets:
            global tcreation
            self._execute(self.tcursor, tcreation)

    def _execute(self, cursor, q):
        cursor.executescript(q)

    def get_user_id(self, screen_name):
        self.tcursor.execute(
            "SELECT uid FROM users WHERE screen_name=? LIMIT 1",
            (screen_name, )
        )

        for row in self.tcursor:
            try:
                return row[0]
            except:
                return -1

    def is_following(self, src_uid, dst_uid):
        for row in self.tcursor.execute(
            "SELECT src_uid FROM followers WHERE src_uid=? AND dst_uid=?",
            (src_uid, dst_uid)):
            return True
        return False

    def get_missing_uid(self):
        cursor = self.tweets.cursor()
        for row in cursor.execute(
            "SELECT dst_uid FROM followers WHERE " \
            "dst_uid NOT IN (SELECT uid FROM users) GROUP BY dst_uid"):
            yield (row[0])


    def get_followers_for(self, uid):
        cursor = self.tweets.cursor()
        for row in cursor.execute(
            "SELECT src_uid FROM followers WHERE dst_uid=?", (uid, )):
            yield (row[0])

    def save_followers(self, leader, ids):
        for id in ids:
            self.tcursor.execute(
                "INSERT OR IGNORE INTO followers(src_uid, dst_uid) " \
                "VALUES (?, ?)", (id, leader)
            )

    def save_user_infos(self, jsonobj):
        self.save_json_obj(jsonobj, type=2)

        for user in jsonobj:
            self.tcursor.execute(
                "INSERT INTO users(uid, screen_name) VALUES (?, ?)",
                (int(user['id_str']), user['screen_name'])
            )

    def save_json_obj(self, jsonobj, type=0):
        if not isinstance(jsonobj, (list, tuple)):
            jsonobj = (jsonobj, )

        for obj in jsonobj:
            try:
                self.dcursor.execute(
                    "INSERT INTO pages(type, uid, tid, json) VALUES(?,?,?,?)",
                    (type, int(obj['user']['id_str']),
                     int(obj['id_str']), json.dumps(obj))
                )
            except Exception:
                print("Skipping tid={:s}. Already present".format(
                      obj['id_str']))

    def save_tweet(self, jsonobj):
        # NB: to convert it back datetime.datetime.fromtimestamp(time.time())
        if not isinstance(jsonobj, (list, tuple)):
            jsonobj = (jsonobj, )

        self.save_json_obj(jsonobj, type=0)

        for tweet in jsonobj:
            rtcount = tweet['retweet_count']
            uid     = int(tweet['user']['id_str'])
            tid     = int(tweet['id_str'])
            txt     = tweet['text']
            geo     = json.dumps(tweet['geo'])
            time    = int(mktime(strptime(tweet['created_at'],
                                          '%a %b %d %H:%M:%S +0000 %Y')))

            if isinstance(rtcount, basestring):
                rtcount = 101

            try:
                self.tcursor.execute(
                    "INSERT INTO tweets(time, rtcount, uid, tid, text, geo) " \
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (time, rtcount, uid, tid, txt, geo)
                )
            except Exception, exc:
                print exc
                print("Skipping tweet. tid={:d} already present".format(tid))

            self.tcursor.execute(
                "INSERT OR REPLACE INTO users(uid, screen_name) " \
                "VALUES (?, ?)", (uid, tweet['user']['screen_name']))

    def save_retweet(self, jsonobj):
        if not isinstance(jsonobj, (list, tuple)):
            jsonobj = (jsonobj, )

        self.save_json_obj(jsonobj, type=1)

        for tweet in jsonobj:
            ruid  = int(tweet['user']['id_str'])
            rtid  = int(tweet['id_str'])
            rtext = tweet['text']
            rgeo  = json.dumps(tweet['geo'])
            rtime = mktime(strptime(tweet['created_at'],
                                      '%a %b %d %H:%M:%S +0000 %Y'))

            otid  = int(tweet['retweeted_status']['id_str'])
            ouid  = int(tweet['retweeted_status']['user']['id_str'])
            otext = tweet['retweeted_status']['text']
            ogeo  = json.dumps(tweet['retweeted_status']['geo'])
            otime = mktime(strptime(tweet['retweeted_status']['created_at'],
                                      '%a %b %d %H:%M:%S +0000 %Y'))

            try:
                self.tcursor.execute(
                    "INSERT INTO retweets(rtime, ruid, rtid, rtext, rgeo, " \
                                         "otime, ouid, otid, otext, ogeo) " \
                                         "VALUES (?,?,?,?,?,?,?,?,?,?)",    \
                    (rtime, ruid, rtid, rtext, rgeo,
                     otime, ouid, otid, otext, ogeo)
                )

            except Exception:
                print("Skipping tweet. rtid={:d} already present".format(rtid))

            # Also update the uid <=> screen_name mappings

            self.tcursor.execute(
                "INSERT OR REPLACE INTO users(uid, screen_name) " \
                "VALUES (?, ?)", (ruid, tweet['user']['screen_name']))

            self.tcursor.execute(
                "INSERT OR REPLACE INTO users(uid, screen_name) " \
                "VALUES (?, ?)", (ouid,
                tweet['retweeted_status']['user']['screen_name']))

    def save(self):
        self.dumps.commit()
        self.tweets.commit()

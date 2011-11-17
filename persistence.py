import json
import os.path
import sqlite3

tcreation = """
CREATE TABLE tweets(
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    tid TEXT UNIQUE,
    text TEXT
);
CREATE TABLE retweets(
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    rtid TEXT UNIQUE,
    rtext TEXT,
    rgeo TEXT,

    authortid TEXT UNIQUE,
    tid TEXT UNIQUE,
    text TEXT,
    geo TEXT
);
CREATE TABLE users(
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    uid TEXT UNIQUE,
    screen_name TEXT UNIQUE,
    geo TEXT
);
CREATE TABLE followers(
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    src_uid TEXT UNIQUE,
    dst_uid TEXT UNIQUE
);
"""

dcreation = """
CREATE TABLE pages(
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    tid TEXT UNIQUE,
    json TEXT
);
"""

class Collector(object):
    def __init__(self):
        missing_dumps = (os.path.exists("dumps.db") == False)
        missing_tweets = (os.path.exists("tweets.db") == False)

        self.dumps = sqlite3.connect('db/dumps.db')
        self.tweets = sqlite3.connect('db/tweets.db')

        self.dcursor, self.tcursor = self.dumps.cursor(), self.tweets.cursor()

        if missing_dumps:
            global dcreation
            self._execute(self.dcursor, dcreation)
        if missing_tweets:
            global tcreation
            self._execute(self.tcursor, tcreation)

    def _execute(self, cursor, q):
        cursor.executescript(q)

    def save_json_obj(self, jsonobj):
        if not isinstance(jsonobj, (list, tuple)):
            coll = (jsonobj, )

        for obj in coll:
            self.dcursor.execute(
                "INSERT INTO pages(tid, json) VALUES(?, ?)",
                (obj['id_str'], json.dumps(obj))
            )

    def save(self):
        self.dumps.commit()
        self.tweets.commit()

if __name__ == "__main__":
    c = Collector()
    c.save_json_obj({'id_str': '123564524352', 'miao': 'fuffa'})
    c.save()

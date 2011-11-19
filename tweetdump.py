import sys
import json
import time
from network import Requester
from persistence import Collector

class TweetDumper(object):
    """
    This class is able to retrieve tweets from the user. If you need to update
    a preexisting database just run with page=0 and interrupt the script as
    soon you see Skipping warning.
    """
    ARGS = ('user', 'page')
    DESC = "Retrieve tweets of <user> starting from <page>"

    METHOD = 'save_tweet'
    URL = "http://twitter.com/statuses/user_timeline/" \
          "{:s}.json?page={:d}&count=100"

    def __init__(self):
        self.url = self.URL
        self.collector = Collector()
        self.invoker = Requester('proxylist')

    def dump(self, politician, page=1):
        try:
            page = int(page)
            while True:
                print("Retrieving tweets at page {:d}".format(page))

                response, content = self.invoker.request(
                    self.url.format(politician, page)
                )

                collection = json.loads(content)

                if len(collection) == 0:
                    break

                meth = getattr(self.collector, self.METHOD)
                meth.__call__(collection)

                page += 1
        finally:
            print("Committing changes to the database")
            self.collector.save()
            self.invoker.save('proxylist')


class RetweetDumper(TweetDumper):
    DESC = "Retrieve retweets of <user> starting from <page>"
    METHOD = 'save_retweet'

    URL = "http://api.twitter.com/1/statuses/retweeted_by_user.json" \
          "?screen_name={:s}&count=100&page={:d}"

class FollowerDumper(TweetDumper):
    ARGS = ('user', 'cursor')
    DESC = "Retrieves the followers of <user> starting from <cursor>"

    METHOD = 'save_friend'
    URL = "http://api.twitter.com/1/followers/ids.json?" \
          "user_id={:d}&cursor={:d}&stringify_ids=true"

    def dump(self, politician, cursor=-1):
        cursor = int(cursor)

        if isinstance(politician, basestring):
            politician = self.collector.get_user_id(politician)

        print("Downloading followers of {:d}".format(politician))

        try:
            while True:
                response, content = self.invoker.request(
                    self.url.format(politician, cursor)
                )

                if response['status'] == '401':
                    # User with protected tweets.
                    break

                body = json.loads(content)
                ids = body['ids']

                if len(ids) == 0:
                    break

                cursor = int(body['next_cursor_str'])
                self.store(politician, ids)

                print("Saving {:d} connections".format(len(ids)))
        finally:
            print("Committing changes to the database")
            self.collector.save()
            self.invoker.save('proxylist')

    def store(self, politician, ids):
        self.collector.save_followers(politician, ids)

class SecondLevelDumper(FollowerDumper):
    ARGS = ('user', '2nd-useridx')
    DESC = "Retrieves the 2nd-level followers of <user> starting " \
           "from <2nd-useridx>"

    def dump(self, politician, page=-1):
        page = int(page)
        pid = self.collector.get_user_id(politician)
        self.pid = pid

        for idx, uid in enumerate(self.collector.get_followers_for(pid)):
            if idx < page:
                continue

            print("Dumping user at index {:d}".format(idx))
            FollowerDumper.dump(self, uid, -1)

    def store(self, dst_uid, ids):
        # Here we need to save only followers which are already present
        ids = filter(
            lambda src_uid: self.collector.is_following(src_uid, self.pid),
            ids
        )
        self.collector.save_followers(dst_uid, ids)


class UserLookups(TweetDumper):
    """
    For the docs and the lulz please refer to:
        https://dev.twitter.com/docs/api/1/get/users/lookup
    """
    ARGS = ()
    DESC = "Fill missing user info information such as screen_name"
    URL = "http://api.twitter.com/1/users/lookup.json?user_id={:s}"

    def dump(self):
        try:
            tmplist = []
            for uid in self.collector.get_missing_uid():
                tmplist.append(uid)

                if len(tmplist) < 100:
                    continue

                self.retrieve_list(tmplist)
                tmplist = []

            if tmplist:
                self.retrieve_list(tmplist)
        finally:
            print("Committing changes to the database")
            self.collector.save()
            self.invoker.save('proxylist')

    def retrieve_list(self, tmplist):
        response, content = self.invoker.request(
                self.url.format(','.join(map(str, tmplist))), 'GET')

        collection = json.loads(content)
        self.collector.save_user_infos(collection)

        print("Saving {:d} profiles".format(len(collection)))

if __name__ == "__main__":
    tools = {
        'tweet': TweetDumper,
        'retweet': RetweetDumper,
        'follower': FollowerDumper,
        '2follower': SecondLevelDumper,
        'infolookup': UserLookups,
    }

    try:
        tool = tools[sys.argv[1]]
        if len(sys.argv) - 2 != len(tool.ARGS):
            raise Exception("love waste")
    except:
        for name, tool in sorted(tools.items()):
            args = ' '.join(map(lambda x: '<{:s}>'.format(x), tool.ARGS))
            print("[+] {:s} {:s} {:s}".format(sys.argv[0], name, args))
            print("")
            print("    {:s}".format(tool.DESC))
            print("")
        sys.exit(-1)
    else:
        tools[sys.argv[1]]().dump(*sys.argv[2:])

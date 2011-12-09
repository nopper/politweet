import sys
import json
from gzip import GzipFile
from network import Requester

class TweetDumper(object):
    URLS = ("http://api.twitter.com/1/statuses/user_timeline.json?" \
            "&count=200&page={:d}",
            "http://api.twitter.com/1/statuses/retweeted_by_user.json" \
            "?count=100&page={:d}")

    def __init__(self, userlist):
        self.filename = userlist
        self.userlist = [int(line.strip()) for line in open(userlist).read().splitlines()]

        self.total = float(len(self.userlist))
        self.current = 0

        self.dumpfile = GzipFile(userlist + ".json.gz", "a")

        self.invoker = Requester()

    def run(self):
        try:
            while self.userlist:
                self.current += 1
                self.dump(self.userlist[0], 1)
                self.userlist.pop(0)
                self.save_progress()
        finally:
            self.save_progress()
            self.close_dump()

    def close_dump(self):
        self.dumpfile.close()

    def save_progress(self):
        f = open(self.filename, "w")
        for i in self.userlist:
            f.write(str(i) + "\n")
        f.close()

    def dump(self, politician, page=1):
        page = int(page)
        oldpage = page
        politician = str(politician)

        msgs = (
            'Retrieving tweets at page {:d} for user {:s}',
            'Retrieving retweets at page {:d} for user {:s}'
        )

        print("Percentage: %.2f" % (self.current / self.total))

        for url, msg in zip(self.URLS, msgs):
            if politician.isdigit():
                url += '&user_id={:s}'
            else:
                url += '&screen_name={:s}'

            while True:
                print(msg.format(page, politician))

                response, content = self.invoker.request(
                    url.format(page, politician)
                )

                if response['status'] == '401':
                    break

                try:
                    collection = json.loads(content)
                except:
                    print "Error decoding json", response['status']
                    continue

                if len(collection) == 0:
                    break

                self.dumpfile.write(content + "\n")

                page += 1

            page = oldpage

if __name__ == "__main__":
    TweetDumper(sys.argv[1]).run()

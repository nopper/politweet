import sys
import json
import socks
import httplib2
from time import mktime, strptime

URL = "http://twitter.com/statuses/user_timeline/{:s}.json?page={:d}&count=100"

class TweetDumper(object):
    def __init__(self, url):
        self.url = url
        self.invoker = httplib2.Http(
            proxy_info=httplib2.ProxyInfo(socks.PROXY_TYPE_SOCKS5,
                                          'localhost', 9050))

    def dump(self, politician, page=1, fmt='{:s}-history'):
        tweetdb = open(fmt.format(politician), 'a')

        while True:
            print("Retrieving tweets at page {:d}".format(page))

            response, content = self.invoker.request(
                    self.url.format(politician, page),
                    'GET', headers={'User-Agent:': 'Google Chrome'})

            if response['status'] != '200':
                print self.url.format(politician, page)
                print("Error at page {:d}. Limit exceeded".format(page))
                break

            collection = json.loads(content)

            if len(collection) == 0:
                print("No more tweets at page {:d}".format(page))
                break

            for tweet in collection:
                txt = tweet['text'].encode('utf-8')
                tweetid = tweet['id_str']
                timeint = mktime(strptime(tweet['created_at'],
                                 '%a %b %d %H:%M:%S +0000 %Y'))

                tweetdb.write("{:s} {:d} {:s}\n".format(
                              tweetid, int(timeint),
                              txt.translate(None, '\n\t'))
                )

            page += 1

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage {:s} <politician> <startpage>".format(sys.argv[0]))
        sys.exit(-1)

    TweetDumper(URL).dump(sys.argv[1], int(sys.argv[2]))

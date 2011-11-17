import sys
from tweetdump import TweetDumper

URL = "http://api.twitter.com/1/statuses/retweeted_by_user.json" \
      "?screen_name={:s}&count=100&page={:d}"

class RetweetDumper(TweetDumper):
    pass

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage {:s} <politician> <startpage>".format(sys.argv[0]))
        sys.exit(-1)

    RetweetDumper(URL).dump(sys.argv[1], int(sys.argv[2]), '{:s}-rtwt')

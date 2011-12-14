import re
import math
from glob import glob

class Similar(object):
    def __init__(self):
        terms = set()
        politician = {}
        ndocs = 0

        for name in glob('*.sorted.merged'):
            user = name[:name.index("-history")]
            politician[user] = self.read(name)

            for term in politician[user]:
                terms.add(term)

            ndocs += 1.0

        terms = list(terms)
        print len(terms)

        def get_df(topic):
            ret = 0
            for user, fdict in politician.items():
                if topic in fdict:
                    ret += 1
            return ret

        #for user, fdict in politician.items():
        #    for topic in fdict:
        #        fdict[topic] *= math.log(ndocs / get_df(topic), 10)

        matrix = []
        for user, fdict in politician.items():
            vector = []

            for termid, term in enumerate(terms):
                vector.append(fdict.get(term, 0))

            normal = math.sqrt(sum([val ** 2 for val in vector]))
            vector = [val / normal for val in vector]

            matrix.append(vector)

        names = politician.keys()
        for uid, name in enumerate(names):
            for tuid, tname in enumerate(names):

                if tuid == uid:
                    continue

                score = 0
                for a, b in zip(matrix[uid], matrix[tuid]):
                    score += a * b

                print "cos(%20s, %20s) = %.5f" % (name, tname, score)

        #print matrix


    def read(self, fname):
        result = {}
        for line in open(fname).readlines():
            freq, topic = re.findall("^\s*(\d+)\s(.*)$", line.strip())[0]
            result[topic] = int(freq)
        return result


class Intersect(object):
    def __init__(self):
        politician = {}

        for name in glob('*.sorted.merged'):
            user = name[:name.index("-history")]
            politician[user] = self.read(name)

        for user in politician.keys():
            set1 = set(politician[user].keys())
            score1 = sum([f for (k,f) in politician[user].items()])

            for target in politician.keys():
                set2 = set(politician[target].keys())
                score2 = sum([f for (k,f) in politician[target].items()])

                if user == target:
                    continue

                inter = set1.intersection(set2)
                scoreinter = 0

                for key in inter:
                    scoreinter += politician[user].get(key, 0)
                    scoreinter += politician[target].get(key, 0)

                union = set1.union(set2)

                #print user, target, float(scoreinter) / (score1 + score2), len(inter)
                print user, target, float(len(inter)) / len(union)
                #print inter


    def read(self, fname):
        result = {}
        for line in open(fname).readlines():
            freq, topic = re.findall("^\s*(\d+)\s(.*)$", line.strip())[0]
            freq = int(freq)

            #if freq < 2:
            #    continue

            result[topic] = int(freq)
        return result

if __name__ == "__main__":
    Intersect()
    Similar()

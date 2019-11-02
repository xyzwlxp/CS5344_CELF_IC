import re

class RDDProcessor:
    def __init__(self, sc, path):
        self.sc = sc
        self.path = path
        self.rdd = self.sc.textFile(self.path)

    def getRddMapping(self):
        query = self.rdd
        list_rdd = query.map(lambda l: (re.split(r'[^\w]+',l)[0], [re.split(r'[^\w]+',l)[1]]))
        input_link_count = list_rdd.reduceByKey(lambda a1, a2: a1+a2)
        return input_link_count


    def computeFrequency(self):
        query = self.rdd
        input_link_count_rdd = query.map(lambda l: (re.split(r'[^\w]+',l)[0], 1))
        out_going_link_count_rdd = query.map(lambda l: (re.split(r'[^\w]+',l)[1], 1))
        input_link_count = input_link_count_rdd.reduceByKey(lambda a1, a2: a1+a2)
        outgoing_link_count = out_going_link_count_rdd.reduceByKey(lambda a1, a2: a1+a2)
        return input_link_count, outgoing_link_count

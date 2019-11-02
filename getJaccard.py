import re

class JaccardProcessor:
    def __init__(self, sc, path):
        self.sc = sc
        self.path = path
        self.rdd = self.sc.textFile(self.path)

    def getJaccardMapping(self):
        query = self.rdd
        list_rdd = query.map(lambda l: (re.split(r'[^\w]+',l)[0], re.split(r'[^\w]+',l)[1]), re.split(r'[^\w]+',l)[2])
        return list_rdd

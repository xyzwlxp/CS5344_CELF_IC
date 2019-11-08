import re
import sys
import string
import os
import math
from pyspark import SparkConf, SparkContext
from rddProcessor import RDDProcessor
from getJaccard import JaccardProcessor
from independentCascade import do_independent_cascade

if len(sys.argv) > 1:
    path = sys.argv[1]
else:
    path = 'data/twitter_combined.txt'

if len(sys.argv) > 2:
    path_graph = sys.argv[1]
else:
    path_graph = 'data/twitter_graph.txt'


if len(sys.argv) > 3:
    out_path = sys.argv[2]
else:
    out_path = 'outFile'
    

out_path_new = out_path
count = 0
while os.path.exists(out_path_new):
    count += 1
    out_path_new = out_path + str(count)

conf = SparkConf()
sc = SparkContext(conf=conf)
rddProcessor = RDDProcessor(sc,path)

rdd_mapping = rddProcessor.getRddMapping()
out_frequency,in_frequency  = rddProcessor.computeFrequency()

# start_node = sc.parallelize([in_frequency.max(lambda l:l[1])[0]]).map(lambda l: (l, 1))
start_node = [in_frequency.max(lambda l:l[1])[0]]

jaccardProcessor = JaccardProcessor(sc, path_graph)

jaccard_mapping = jaccardProcessor.getJaccardMapping()


node_result = do_independent_cascade(jaccard_mapping, start_node)

#rdd2 = rdd_mapping.sortByKey()
print(start_node)

print(node_result)

#frequency[0].saveAsTextFile(out_path_new)
sc.stop()

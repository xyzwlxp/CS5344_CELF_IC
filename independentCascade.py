import sys
import random

def do_indepenedent_cascade(self, rdd, start_node):
    activated_node_list = start_node
    prev_activated_node_list = activated_node_list
    while len(prev_activated_node_list) != len(activated_node_list):
        nodes = rdd.filter(lambda l: activated_node_list.contains(l[0]))
        nodes_mapped = nodes.map(lambda l: (l[1], l[2]))
        rdd_reduced = nodes_mapped.reduceByKey(lambda l1,l2: l1+l2)
        for node in rdd_reduced.keys():
            if random.random() < rdd_reduced.get(node):
                activated_node_list.append(node)

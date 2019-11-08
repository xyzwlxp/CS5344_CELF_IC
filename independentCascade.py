import sys
import random
import time
def do_independent_cascade(rdd, start_node):
    activated_node_list = start_node
    all_activated_node_list = activated_node_list
    start_time = time.time()
    visited = set()
    while True: 
        # prev_activated_node_list = activated_node_list
        # print("1--- %s seconds ---" % (time.time() - start_time))
        # rdd = rdd.sortByKey()
        # print("1--- %s seconds ---" % (time.time() - start_time))
        # nodes = rdd.filter(lambda l: activated_node_list.lookup(l[0])).filter(lambda l: l[1] not in visited)
        # print("1--- %s seconds ---" % (time.time() - start_time))
        # nodes_mapped = nodes.map(lambda l: (l[1][0], l[1][1]))
        # print("1--- %s seconds ---" % (time.time() - start_time))
        # rdd_reduced = nodes_mapped.reduceByKey(lambda l1,l2: float(l1)+float(l2))
        # print("1--- %s seconds ---" % (time.time() - start_time))
        # visited.extend(rdd_reduced.keys().collect())
        # activated_nodes = rdd_reduced.filter(lambda l: random.random() < float(l[1]))
        # print("1--- %s seconds ---" % (time.time() - start_time))
        # activated_node_list = activated_node_list.union(activated_nodes)
        # print("1--- %s seconds ---" % (time.time() - start_time))
        # if prev_activated_node_list.count() == activated_node_list.count():
        #     break

        print("1--- %s seconds ---" % (time.time() - start_time))
        nodes = rdd.filter(lambda l: l[0] in activated_node_list).filter(lambda l: l[1] not in visited)
        nodes_mapped = nodes.map(lambda l: (l[1], l[2]))
        rdd_reduced = nodes_mapped.reduceByKey(lambda l1,l2: float(l1)+float(l2))
        print("2--- %s seconds ---" % (time.time() - start_time))
        visited = list(visited)
        visited.extend(rdd_reduced.keys().collect())
        visited = set(visited)
        activated_node_list = rdd_reduced.filter(lambda l: random.random() < float(l[1])).keys().collect()
        all_activated_node_list.extend(activated_node_list)
        activated_node_list = set(activated_node_list)
        print(len(all_activated_node_list))
        if len(activated_node_list) == 0:
            break
    return activated_node_list

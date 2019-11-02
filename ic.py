import networkx as nx
import ndlib.models.ModelConfig as mc
import ndlib.models.epidemics as ep
import time

g = nx.DiGraph()

with open('twitter_graph.txt', 'r') as f:
    for row in f:
        row = row.rstrip()
        if not row:
            continue
        
        ls = row.split()
        source = ls[0]
        to = ls[1]
        score = float(ls[2])
        g.add_edge(source, to, weight=score)

# import ndlib.models.ModelConfig as mc

# # Model Configuration
# config = mc.Configuration()

# infected_nodes = [0, 1, 2, 3, 4, 5]
# config.add_model_initial_configuration("Infected", infected_nodes)

# Network topology
# g = nx.erdos_renyi_graph(1000, 0.1)

# Model selection
model = ep.IndependentCascadesModel(g)

# Model Configuration
config = mc.Configuration()
#config.add_model_parameter('fraction_infected', 0.1)
infected_nodes = ['70775228', '70775228', '70775228']
config.add_model_initial_configuration("Infected", infected_nodes)

# Setting the edge parameters
threshold = 0.5
for e in g.edges():
    config.add_edge_configuration("threshold", e, threshold)

model.set_initial_status(config)

print("start iteration")
t0 = time.time()
# Simulation execution
iterations = model.iteration_bunch(100)

print(iterations)

print('takes {}s'.format(time.time()-t0))

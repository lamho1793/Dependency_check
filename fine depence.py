import json
from pickle import FALSE
from sys import displayhook
import warnings
import networkx as nx
import pandas as pd
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
import regex as re
import scipy as sp
import timeit
import matplotlib.pyplot as plt

def Find_depence(name):
    warnings.filterwarnings('ignore')
    # name = input("Query task:")
    G=nx.read_graphml("Backward.graphml")

    SAMPEL=nx.single_source_dijkstra_path(G,f'''{name}''')
    store=list(SAMPEL.keys())

    return store

    # node_color_map=[]
    # for x in store:
    #     if x == f'''{name}''':
    #         node_color_map.append("red")
    #     else:
    #         node_color_map.append("blue")


    # with open("sample.json", "w+") as outfile:
    #     outfile.write(json.dumps(SAMPEL))
    # H1 = nx.subgraph(G, store)

    # pos1 = nx.spring_layout(H1, scale=80)
    # nx.draw_networkx_nodes(H1, pos1, node_shape='o', node_color=node_color_map, alpha=0.5, node_size=80)
    # nx.draw_networkx_edges(H1, pos1, style='solid', alpha=0.4,)
    # # nx.draw(H1, with_labels=False, node_color=node_color_map, font_size=8)
    # plt.show()
start = timeit.default_timer()  
GP_Importance_task=pd.read_csv('GP_before.csv', header=0)

dep={}
for x in GP_Importance_task.values:
    try:
        dep[x[0]]=Find_depence(x[0])
    except:
        print(x[0])
with open("sample.json", "w+") as outfile:
    outfile.write(json.dumps(dep))

temp=[]
for x in dep:
    temp+=dep[x]

temp=list(set(temp))
with open("sample1.json", "w+") as outfile:
    outfile.write(json.dumps(temp))

stop = timeit.default_timer()
print('Time: ', stop - start) 

# Find_depence()
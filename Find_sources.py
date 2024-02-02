from itertools import count
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
from pyvis.network import Network

warnings.filterwarnings('ignore')
name = input("Target task:")
start = timeit.default_timer()

G=nx.read_graphml("Backward.graphml")

all_source=list(nx.dfs_preorder_nodes(G,name))
# all_source=[]
# for x in list(G.nodes):
#     temp=nx.single_source_dijkstra_path(G,x)
#     try:
#         all_source+=temp[f'''{name}''']
#     except:
#         continue
# node_color_map=[]
# all_source=list(set(all_source))

# print(len(all_source))

nt = Network('100%', '100%', directed=True)
H1 = nx.subgraph(G, all_source)

for x in all_source:
    if x == name:
        H1.nodes[x]['group']=1
    else:
        H1.nodes[x]['group']=2

nt.from_nx(H1)
nt.options.edges.toggle_smoothness("dynamic")
nt.options.layout.set_tree_spacing(100)
# nt.show_buttons(filter_=['nodes', 'edges', 'physics'])
nt.show('nx.html')
with open("all_source.json", "w+") as outfile:
    outfile.write(json.dumps(all_source))

# H1 = nx.subgraph(G, all_source)
# stop = timeit.default_timer()
# print('Time: ', stop - start) 
# print(nx.is_arborescence(H1))
# #nx.draw(H1, with_labels=False, font_size=8)
# pos1 = nx.spring_layout(H1, scale=20, k=3/np.sqrt(H1.order()))
# nx.draw_networkx_nodes(H1, pos1, node_shape='o', node_color=node_color_map, alpha=0.5, node_size=80)
# nx.draw_networkx_edges(H1, pos1, style='solid', alpha=0.4, )
# # nx.draw(H1, pos=pos1, with_labels=False, node_color=node_color_map, font_size=8)
# plt.show()

#TGR_CRITICAL_REPORT_CHBN_C

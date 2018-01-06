import json
import pickle

import networkx as nx
from networkx.readwrite import json_graph

import http_server
from Utils import GraphObjectEncoder


gdm = nx.MultiDiGraph()
gdm.add_node(1)
gdm.add_node(2)
gdm.add_edge(1,2, attr_dict={'name':'a', 'support': 0 })
gdm.add_edge(1,2,key="uri_uniqu" ,attr_dict={ 'support': 0 })
b = gdm.has_edge(1,2, key="uri_uniqus")
bk= gdm.has_edge(1,2, key="uri_uniqu")
gdm.add_edge(1,2,key="uri_uniqu" , attr_dict={ 'support': 1})
gdm.add_edge(1,2,key="uri_uniqu" )
gdm[1][2]["uri_uniqu"]['support']+=1

#gdm.add_edge(1,2, attr_dict={'name':'b', 'support': 0 })
#gdm.add_edge(1,2, attr_dict={'name':'c', 'support': 0 })
gdm.add_edge(1,2, attr_dict={'name':'d', 'support': 0 })



if __name__ == '__main__':




    tg_file = open("comedian/comedian_pg.dump" , 'r')
    tg = pickle.load(tg_file)
    tg_file.close()



    # this d3 example uses the name attribute for the mouse-hover value,
    # so add a name to each node
    G = tg.graph
    i=7
    for n in G:
        if 'obj' in G.node[n]:
            G.node[n]['name'] = (G.node[n]['obj'].title).encode('utf-8')
        if n in tg.type_dict:
            G.node[n]['group'] = 7
        elif n == tg.uri:
            G.node[n]['group'] = 1
        else:
            G.node[n]['group'] = 3

    # write json formatted data
    d = json_graph.node_link_data(G)  # node-link format to serialize
    # write json
    json.dump(d, open('force/force.json', 'w'), cls=GraphObjectEncoder)
    print('Wrote node-link JSON data to force/force.json')
    # open URL in running web browser
    http_server.load_url('force/force.html')
    print('Or copy all files in force/ to webserver and load force/force.html')
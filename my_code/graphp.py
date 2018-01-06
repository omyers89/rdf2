
import http_server

import json
import networkx as nx
from networkx.readwrite import json_graph

import logging, sys
from Utils import GraphObjectEncoder

DEBUG = False

class SubjectGraph():
    def __init__(self, new_uri):
        self.uri = new_uri
        self.graph = nx.MultiDiGraph()
        self.graph.add_node(self.uri)
        self.type_dict = {}
        self.rel_dict = {}
        #self.grph_objs = {}

        #self.prop_objects = {}
        #self.type_objects = {}

    def add_prop(self, new_prop):
        #if prop exist update counter.

        if new_prop not in self.graph:
            prop_node = PropertyNode(new_prop)
            #prop_node.support += 1
            self.graph.add_node(new_prop, obj=prop_node)
            # self.graph[new_prop]['object'] = prop_node
            self.graph.add_edge(self.uri, new_prop)
            #self.prop_objects[new_prop] = 1
            logging.info('new prop was added: ' + new_prop)

        self.graph.node[new_prop]['obj'].support += 1
        #self.prop_objects[new_prop] += 1
        logging.info('prop was updated: ' + new_prop)

    def normalize_graph(self, totals, unis, singles, p_uri, p_cnt):
        # divide every atribute of the support with the number at totals
        # for tnode, dat in self.graph.nodes(data=True):
        #     if tnode == self.uri:
        #         continue
        #     if 'obj' in dat:
        #         dat['obj'].norm(totals)
        #         if tnode in unis:
        #             dat['obj'].is_unique = True
        #         if tnode in singles:
        #             dat['obj'].is_single = True
        if DEBUG:
            if totals > p_cnt:
                print "ok, totals is bigger than p-cnt"
        for eg, atp in self.rel_dict.items():
            fn, tn , uri = eg
            #prop_count = self.prop_objects[atp]
            if self.graph.has_edge(fn, tn , uri) and atp == p_uri:
                tut = self.graph[fn][tn][uri]['support']
                retio = float(tut) / p_cnt
                if retio < 0.01:
                    if DEBUG:
                        if fn == 'http://dbpedia.org/ontology/City@http://dbpedia.org/ontology/birthPlace' \
                                and tn == 'http://dbpedia.org/ontology/City@http://dbpedia.org/ontology/birthPlace':
                            if uri == "http://dbpedia.org/ontology/isPartOf":
                                print "****just removed city birthPlace isPartOf. tut:{}, p_cnt:{}, retio:{}".format(tut,p_cnt, retio)

                    self.graph.remove_edge(fn,tn,uri)
                   #if fn == 'http://dbpedia.org/ontology/City@http://dbpedia.org/ontology/birthPlace'
                else:
                    self.graph[fn][tn][uri]['support'] = min(float(tut) / p_cnt, 1)



    def reset_types(self):
        for t in self.type_dict:
            self.type_dict[t] = False


    def add_type_to_prop(self, prop_uri, new_type):
        # add new type if type found adds 1 to support
        if prop_uri not in self.graph:
            logging.info('prop not in graph: ' + prop_uri)
            return

        new_type_p = new_type + '@' + prop_uri
        if new_type_p not in self.graph:
            self.type_dict[new_type_p] = False
            type_node = TypeNode(new_type, prop_uri)

            self.graph.add_node(new_type_p, obj=type_node)
            # self.graph[new_prop]['object'] = prop_node
            self.graph.add_edge(prop_uri, new_type_p)

        self.graph.node[new_type_p]['obj'].support += 1


    def reset_uniques(self):
        pass

    def update_uniques(self):
        pass

    def add_relations(self, from_type, to_type, at_prop, relations):
        for rel_uri in relations:
            self.add_relation(from_type, to_type, at_prop, rel_uri)




    def add_relation(self, from_type, to_type, at_prop, relatio_uri):
        '''
        gets 2 types and the rilation between them and add it as an edge to the graph.
        :param from_type:
        :param to_type:
        :param relatio_uri: the uri for the relation to be added
        :return:
        '''

        if at_prop in self.graph:
            types = self.graph.neighbors(at_prop)
            fn = from_type + '@' + at_prop
            tn = to_type + '@' + at_prop
            if fn in types and tn in types:
                if self.graph.has_edge(fn,tn, relatio_uri):
                    self.graph[fn][tn][relatio_uri]['support'] += 1
                else:
                    self.graph.add_edge(fn, tn, key=relatio_uri, attr_dict={ 'support': 1 })
                    self.rel_dict[(fn, tn, relatio_uri)] = at_prop


                logging.info('relation added at: ' + fn + ';' + tn + ';' + relatio_uri)



    def update_relation(self):
        pass



def evaluate_selection(rule_graph, sub_graph):

    xdiff = 0
    diff = 0
    max_ratio = 0
    for t in sub_graph.type_dict:
        if t in rule_graph.type_dict:
            curr_ratio = rule_graph.graph.node[t]['obj'].ratio
            diff += abs( curr_ratio - sub_graph.graph.node[t]['obj'].support)
            if max_ratio < curr_ratio:
                max_ratio = curr_ratio
        else:
            xdiff += 1

    xdiff *= max_ratio

    max_ratio = 0
    xrdiff = 0
    rdiff = 0



    for r in sub_graph.rel_dict:
        if r in rule_graph.rel_dict:
            fn, tn, relatio_uri = r
            curr_ratio = rule_graph.graph[fn][tn][relatio_uri]['support']
            rdiff += abs(curr_ratio - sub_graph.graph[fn][tn][relatio_uri]['support'])
            if max_ratio < curr_ratio:
                max_ratio = curr_ratio
        else:
            xrdiff += 1

    xrdiff *= max_ratio

    total_diff = xdiff + diff + xrdiff + rdiff

    return total_diff





def normalize_graph(G, totals, unis, singles):
    # divide every atribute of the support with the number at totals
    for tnode, dat in G.graph.nodes(data=True):
        if tnode == G.uri:
            continue
        if 'obj' in dat:
            dat['obj'].norm(totals)
        if tnode in unis:
            dat['obj'].is_unique = True
        if tnode in singles:
            dat['obj'].is_single = True

    for eg in G.rel_dict:
        fn, tn, uri = eg
        ratio = G.graph[fn][tn][uri]['support']
        G.graph[fn][tn][uri]['support'] = float(ratio) / totals

class GraphObject():
    def __init__(self, new_uri):
        self.uri = new_uri
        self.title = self.uri.rsplit('/', 1)[-1]
        self.support = 0
        self.ratio = -1

    def __str__(self):
        return self.title

    def norm(self,tot):

        self.ratio = min(float(self.ratio) / tot, 1)


class PropertyNode(GraphObject):

    def __init__(self, new_uri):
        GraphObject.__init__(self, new_uri)
        self.is_single = False

    def __hash__(self):
        return hash(self.uri)




class TypeNode(GraphObject):


    def __init__(self, new_uri, parent_prop):
        GraphObject.__init__(self, new_uri)
        self.is_unique = False

        self.checked = False
        self.uniques = 0
        self.prop_uri = parent_prop

    def __hash__(self):
        return hash(self.uri + '@' +self.prop_uri)

    def norm(self, tot):
        self.ratio = float(self.ratio) / tot

        self.uniques = float(self.uniques) / tot

    def uniqueness(self):
        return  1 if self.is_unique else 0



class Relation(GraphObject):
    def __init__(self, new_uri):
        GraphObject.__init__(self, new_uri)

    def __hash__(self):
        return hash(self.uri)








if __name__ == '__main__':
    '''
    TODO:
    1. make proper graph object.
    2. proper inheritence
    3. dict functions - test before going crazy with it.
    4. make sure type is hashed with the right  property
    5. maintain the weight and suport functions
    6. run the shit to find main properties of politician and soccer player
    7. put it on the web
    8. update inconsistencies
    9. print incs.

    '''
    DEBUG = True



    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    ng = SubjectGraph("Politician")
    ng.add_prop("birthPlace")
    ng.add_prop("deathPlace")
    ng.add_prop("party")
    ng.add_prop("predecessor")
    ng.add_prop("successor")
    ng.add_type_to_prop("birthPlace", "City")
    ng.add_type_to_prop("birthPlace", "Country")
    ng.add_type_to_prop("deathPlace", "City")
    ng.add_type_to_prop("deathPlace", "Country")
    ng.add_type_to_prop("party", "PoliticalPartiesInTheNetherlands")
    ng.add_type_to_prop("successor", "UnitedStatesSenatorsFromSouthCarolina")
    ng.add_type_to_prop("successor", "DemocraticPartyStateGovernors")
    ng.add_type_to_prop("predecessor", "NewYorkLawyers")
    ng.add_type_to_prop("predecessor", "CurrentNationalLeaders")


    ng.add_relation("City", "Country","birthPlace" ,"located_in")
    ng.add_relation("City", "Country","birthPlace" ,"located_in")
    ng.add_relation("City", "Country","deathPlace" ,"located_in")
    ng.add_relation("City", "Country","deathPlace" ,"located_in")


    # tots = 20
    # p_c = 20
    # uni = {'City'}
    # singlet = {'dateOfBirth'}
    # ng.normalize_graph(tots, uni, singlet, "placeOfDeath", p_c)
    # ng.normalize_graph(tots, uni, singlet, "dateOfBirth", p_c)

    G = ng.graph
    p1 = G.nodes()
    # this d3 example uses the name attribute for the mouse-hover value,
    # so add a name to each node

    # i=7
    # for n in G:
    #     i*=2
    #     G.node[n]['name'] = str(n)
    #     G.node[n]['value'] = i
    #     G.node[n]['group'] = i/2


    type_dict = { "birthPlace":0,"deathPlace":0,"party":0,"predecessor":0,"successor":0}
    uri = "Politician"
    for n in G:
        G.node[n]['name'] = str(n)
        if n in type_dict:
            G.node[n]['group'] = 7
        elif n == uri:
            G.node[n]['group'] = 1
        else:
            G.node[n]['group'] = 3

    # write json formatted data
    d = json_graph.node_link_data(G)  # node-link format to serialize
    # write json
    json.dump(d, open('force/force.json', 'w'), cls=GraphObjectEncoder)
    print('Wrote node-link JSON data to force/force.json')
    # open URL in running web browser
 #   http_server.load_url('force/force.html')
    print('Or copy all files in force/ to webserver and load force/force.html')
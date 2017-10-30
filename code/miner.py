from SPARQLWrapper import SPARQLWrapper, JSON
import pickle
import sys
import os
import time
import graphp
from threading import Thread
from Utils import *

DBPEDIA_URL = "http://tdk3.csf.technion.ac.il:8890/sparql"
DBPEDIA_URL_UP = "http://dbpedia.org/sparql"
SMAL_URL = "http://cultura.linkeddata.es/sparql"
DEBUG = False
PROFILER = True

class miner():

    def __init__(self, kb, subj, s_uri):
        self.knowledge_base = kb
        self.subject = subj
        self.subject_uri = s_uri
        self.sparql = SPARQLWrapper(kb)
        self.RG = graphp.SubjectGraph(s_uri)
        self.p_rels = {}
        self.timers = {'get_os': 0, 'update_so_dict': 0, 'get_dbo_ts': 0}


    def get_ot_unique_dict(self, o_list, o_dict_t):
        res_dict = {}
        # single= False
        # if len(os) == 1:
        #     single = True
        for o in o_list:
            if o in o_dict_t:
                for t in o_dict_t[o]:
                    #if (t in res_dict) or single:
                    if t in res_dict:
                        res_dict[t] = False #this is the second time t in res_dict so not unique!
                    else:
                        res_dict[t] = True #this is the first time t in res_dict so unique so far!
        return res_dict

    def get_ot_unique_dict_rel(self, o_list, o_dict_t):
        res_dict = {}
        # single= False
        # if len(os) == 1:
        #     single = True
        for o in o_list:
            if o in o_dict_t:
                for t in o_dict_t[o]:

                    # if (t in res_dict) or single:
                    if t in res_dict:
                        (res_dict[t])[0]=False   # this is the second time t in res_dict so not unique!
                        (res_dict[t])[1].append(o)
                        #print "added o not unique, res_dict is for type:" + t
                        #print res_dict[t]
                    else:
                        res_dict[t] = [True,[o]]  # this is the first time t in res_dict so unique so far!
                        #print "added o  unique, res_dict is for type:" + t
                        #print res_dict[t]
        return res_dict


    def update_pt(self, t_dict_t,p_unique_t_dict):
        """
        the function count the uniqueness of types for a specific object and add it to the total statistics
        about the property & type
        :param t_dict_t: dictionary for all types that appears together with a specific property and true/false for
        uniqueness
        :param p_unique_t_dict: for every p the total statistics so far
        :return: just update the dictionary.
        """
        for t, v in t_dict_t.items():
            if t not in p_unique_t_dict:
                p_unique_t_dict[t] = {'pos': 0, 'tot': 0}
            if v:
                p_unique_t_dict[t]['pos'] += 1
            p_unique_t_dict[t]['tot'] += 1


    def get_ts_for_o(self, o_list):
        """
        Given list of object and a specific knowledge base creates a dictionary of o and the list of dbo:type that
        defines it

        :param o_list: list of object for specific relation
        :param db: the KB we query
        :return: o_dict dictionar {'<object>' : [c1,c2,c3...] (type list)
        """
        if PROFILER:
            t0 = time.time()
        o_dict = {}
        for o in o_list:
            o_dict[o] = []
            self.sparql.setQuery("""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT  DISTINCT ?t
            WHERE{
              <%s>  a   ?t .
                FILTER (regex(?t, "^http://dbpedia.org/", "i"))
                FILTER NOT EXISTS {
                    ?subtype ^a ?o ;
                    rdfs:subClassOf ?t .
                    FILTER ( ?subtype != ?t )
                }
            } """ % o)

            #need to filter the types to informative ones.
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()

            for result in results["results"]["bindings"]:
                c = result["t"]["value"]
                o_dict[o].append(c)
        if PROFILER:
            t1 = time.time()
            total_time = t1 - t0
            self.timers['get_os'] += total_time
        return o_dict


    def get_dbots_for_o(self, o_list):

        """
        Given list of object and a specific knowledge base creates a dictionary of o and the list of dbo:type that
        defines it

        :param o_list: list of object for specific relation
        :param db: the KB we query
        :return: o_dict dictionar {'<object>' : [c1,c2,c3...] (type list)
        """
        if PROFILER:
            t0 = time.time()
        o_dict = {}
        for o in o_list:
            o_dict[o] = []
            self.sparql.setQuery("""
                                SELECT  DISTINCT  ?c
                                WHERE{
                                    <%s>  <http://dbpedia.org/ontology/type>   ?c .
                                    FILTER regex(?c, "^http://dbpedia.org", "i")
                                }
                            """ % o)

            # need to filter the types to informative ones.
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()

            for result in results["results"]["bindings"]:
                c = result["c"]["value"]
                o_dict[o].append(c)
        if PROFILER:
            t1 = time.time()
            total_time = t1 - t0
            self.timers['get_dbo_ts'] += total_time
        return o_dict

    def update_graph(self,s, p , t_dict):
        for t, u in t_dict.items():
            self.RG.add_type_to_prop(p, t, u)

        query_text = ("""
                        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                        SELECT distinct  ?t1 ?r12 ?r21 ?t2
                        WHERE {
                        <%s> <%s> ?o1;
                              <%s> ?o2.
                        FILTER (?o1 < ?o2).
                        ?o1 a ?t1.
                        ?o2 a ?t2.

                        OPTIONAL {
                        ?o1 ?r12 ?o2.
                        ?o2 ?r21 ?o1.
                        }

                        FILTER (regex(?t1, "^http://dbpedia.org/", "i")).
                        FILTER (regex(?t2, "^http://dbpedia.org/", "i")).
                        FILTER NOT EXISTS {
                            ?subtype1 ^a ?o1 ;
                            rdfs:subClassOf ?t1 .
                            FILTER ( ?subtype1 != ?t1 )
                            }
                        FILTER NOT EXISTS {
                            ?subtype2 ^a ?o2 ;
                            rdfs:subClassOf ?t2 .
                            FILTER ( ?subtype2 != ?t2 )
                            }
                        FILTER regex(?r12, "^http://dbpedia.org/ontology", "i").
                        FILTER regex(?r21, "^http://dbpedia.org/ontology", "i").

                    }""" % (s, p, p))

        # I figured out that a good filter for the type of the object has to  be of "^http://dbpedia.org/ontology"
        # in oreder to get valuable results
        self.sparql.setQuery(query_text)
        self.sparql.setReturnFormat(JSON)
        results_inner = self.sparql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            t1 = inner_res["t1"]["value"]
            t2 = inner_res["t2"]["value"]
            r12 = inner_res["r12"]["value"]
            r21 = inner_res["r21"]["value"]
            if r12 == "":
                r12 = "None"
            self.RG.add_relation(t1,t2,p,r12)
            if r21 == "":
                r21 = "None"
            self.RG.add_relation(t2,t1,p,r21)


    def get_rels(self, from_o, to_o):
        new_rels = []
        query_text = ("""

                    SELECT distinct  ?r
                    WHERE {
                    <%s> ?r <%s>.
                    FILTER regex(?r, "^http://dbpedia.org/ontology", "i").
                }""" % (from_o, to_o))


        # I figured out that a good filter for the type of the object has to  be of "^http://dbpedia.org/ontology"
        # in oreder to get valuable results
        self.sparql.setQuery(query_text)
        self.sparql.setReturnFormat(JSON)
        results_inner = self.sparql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:

            r = inner_res["r"]["value"]

            if r == "":
                r = "None"
            new_rels.append(r)

        return new_rels




    def update_graph_rel(self,p, o_list, ot_dict):
        for o, ts in ot_dict.items():
            for t in ts:
                self.RG.add_type_to_prop(p, t) #, u) ********fix this in graph

        o_len = len(o_list)
        for i in range(1,o_len):
            for j in range(i, o_len):
                o1=o_list[i-1]
                o2=o_list[j]
                relations_o12 = self.get_rels(o1, o2)
                relations_o21 = self.get_rels(o2, o1)
                for t1 in ot_dict[o1]:
                    for t2 in ot_dict[o2]:
                        self.RG.add_relations(t1, t2, p, relations_o12)
                        self.RG.add_relations(t2, t1, p, relations_o21)

    def add_rels_to_p(self, p, relations_o21, relations_o12):
        if p not in self.p_rels:
            self.p_rels[p]={}
        for r in relations_o21:
            if r not in self.p_rels[p]:
                self.p_rels[p][r]=0
            self.p_rels[p][r] += 1

        for r in relations_o12:
            if r not in self.p_rels[p]:
                self.p_rels[p][r] = 0
            self.p_rels[p][r] += 1


    def update_p_rel(self, p, o_list):
        o_len = len(o_list)
        for i in range(1, o_len):
            for j in range(i, o_len):
                o1 = o_list[i-1]
                o2 = o_list[j]
                relations_o12 = self.get_rels(o1, o2)
                relations_o21 = self.get_rels(o2, o1)
                self.add_rels_to_p(p, relations_o21, relations_o12)


    def get_sub_graph(self,s, p_dict, quick = False):
        sinles = {}
        for p in p_dict:
            self.RG.add_prop(p)
            o_list = self.update_so_dict(p, s)
            ot_dict = self.get_ts_for_o(o_list)
            t_dict = self.get_ot_unique_dict(o_list,
                                             ot_dict)  # Done: for specific person and property find the unique types!
            if len(o_list) == 1:
                sinles[p] = 1
            self.update_graph(s, p, t_dict)

        self.RG.normalize_graph(1, {}, sinles)
        return self.RG

    def normalize_p_rels(self,p,p_count):
        if p not in self.p_rels:
            return
        for rel in self.p_rels[p]:
            if p_count != 0:
                self.p_rels[p][rel] /= p_count


    def mine_rules(self, quick, min_pos_th=0.2, positive_total_ratio_th=0.8):
        print "mining rules for {}".format(self.subject)
        s_dump_name = "../results/" + self.subject + "/" + self.subject + "_top.dump"
        p_dump_name = "../results/" + self.subject + "/" + self.subject + "_prop.dump"
        # get the 100 most popular properties for type person in dbp
        p_dict = self.get_p_dict_from_dump(quick, p_dump_name)
        s_dict = self.get_s_dict_from_dump(quick, s_dump_name)
        rules70_ = {}
        rules70_dbo = {}
        rules60_70 = {}
        rules50_60 = {}
        rules_wierd = {}
        rules_wierd_dbo = {}
        one_of_a_kind = {}
        low_props = {}
        progress = 0
        p_size = len(p_dict)
        t0 = time.time()
        p_indx = 0
        for p in p_dict:
            self.RG.add_prop(p)
            #s_dict = {}
            #this dictionary holds the statistics for every p separately p_unique_t_dict[t]={'pos': #uniqueness, 'tot': #totalappearence}
            p_unique_t_dict = {}
            p_unique_dbot_dict = {}
             #s is a sepecific person and os=[o1,o2,o3] is the list of objects that are in the relation: P(s,o)
            #
            p_count = 0
            # for every person in the list (2000 in total)
            p_only_one = 0
            for i,s  in enumerate(s_dict):

                o_list = self.update_so_dict(p, s)
                if len(o_list) > 0:
                    p_count += 1
                ot_dict = self.get_ts_for_o(o_list)
                odbot_dict = self.get_dbots_for_o(o_list)

                #t_dict_rel = self.get_ot_unique_dict_rel(o_list, ot_dict)  # Done: for specific person and property find the unique types!
                t_dict = self.get_ot_unique_dict(o_list, ot_dict)  # Done: for specific person and property find the unique types!
                dbo_t_dict = self.get_ot_unique_dict(o_list, odbot_dict)  # Done: for specific person and property find the unique types!

                if len(o_list) > 1:
                    #ot_dict is list of types for every o in the list for specific person and property

                    self.update_pt(t_dict,p_unique_t_dict) #Done: add up the times that t was unique for the specific p
                    self.update_pt(dbo_t_dict,p_unique_dbot_dict) #Done: add up the times that t was unique for the specific p
                elif len(o_list) == 1:
                    p_only_one += 1

                #self.update_graph(s, p, t_dict) - move to end of s dict
                #self.update_graph_rel(p, o_list, ot_dict)
                self.update_p_rel(p, o_list)
                if DEBUG:
                    txt = "\b S loop progress: {}".format(i)
                    sys.stdout.write(txt)
                    sys.stdout.write("\r")
                    sys.stdout.flush()

            if DEBUG:
                sys.stdout.write("\b the total p are : {}".format(p_count))
                sys.stdout.write("\r")
                sys.stdout.flush()
            p_indx += 1
            #print total_totals
            for t, counts in p_unique_t_dict.items():
                pos = float(counts['pos'])
                tot = float(counts['tot'])
                data = {'p': p, 't': t, 'pos': pos, 'tot': tot}
                if tot != 0:
                    data['ratio'] = pos / tot
                t_key = t + '@' + p
                if float(p_count)/len(s_dict) > 0.1:
                    if p_count > 0:
                        #if (tot/p_count) >= min_pos_th:
                        if (tot >= 5):
                            if ((pos /tot) >= positive_total_ratio_th) :
                                rules70_[t_key] = data
                            elif((pos /tot) >= 0.75):
                                rules60_70[t_key] = data
                            elif((pos /tot) >= 0.6):
                                rules50_60[t_key] = data
                        else:
                            rules_wierd[t_key] = data
                else:
                    low_props[t_key] = data

            for to, countso in p_unique_dbot_dict.items():
                pos = float(countso['pos'])
                tot = float(countso['tot'])
                datao = {'p': p, 't': to, 'pos': pos, 'tot': tot}
                if tot != 0:
                    datao['ratio'] = pos / tot
                t_key = to + '@' + p
                if float(p_count) / len(s_dict) > 0.1:
                    if p_count > 0:
                        # if (tot/p_count) >= min_pos_th:
                        if (tot >= 5):
                            if ((pos / tot) >= positive_total_ratio_th):
                                rules70_dbo[t_key] = datao
                        else:
                            rules_wierd_dbo[t_key] = datao


            if float(p_count) / len(s_dict) > 0.1:
                if p_count > 0:
                    p_once_ratio = float(p_only_one)/p_count
                    if  p_once_ratio > 0.8:
                        one_of_a_kind[p] = p_once_ratio
            progress += 1

            self.normalize_p_rels(p, p_count)
            #self.RG.normalize_graph(len(s_dict), rules70_, one_of_a_kind,p,  p_count)
            if DEBUG:
                txt = "\b Properties progress:{} / {} ".format(progress, p_size)
                sys.stdout.write(txt)
                sys.stdout.write("\r")
                sys.stdout.flush()

        all_rules_list = (rules70_, rules60_70, rules70_dbo, rules_wierd, rules_wierd_dbo, one_of_a_kind, low_props)

        dir_name = "../results/" + self.subject
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        dump_name = dir_name + "/" + self.subject + "_rules.dump"
        r_dict_file = open(dump_name, 'w')
        pickle.dump(all_rules_list, r_dict_file)
        r_dict_file.close()

        t1 = time.time()
        total_time = t1 - t0
        avg_time = float(total_time) / p_size
        print "mining done for {} times are:".format(self.subject)
        print "total time: {} ".format(total_time)
        print "avg time per property: {}".format(avg_time)
        print "time at get_os: {}".format(self.timers['get_os'])
        print "time at update_so_dict: {}".format(self.timers['update_so_dict'])
        return all_rules_list


    def update_so_dict(self, p, s):
        if PROFILER:
            t0 = time.time()
        o_list = []
        query_text = ("""
                    SELECT DISTINCT ?o
                    WHERE{
                            <%s> <%s> ?o .
                            ?o a ?t .

                        } """ % (s, p))
        #FILTER (regex(?t, "^http://dbpedia.org/")) maybe removed
        # I figured out that a good filter for the type of the object has to  be of "^http://dbpedia.org/ontology"
        # in oreder to get valuable results
        self.sparql.setQuery(query_text)
        self.sparql.setReturnFormat(JSON)
        results_inner = self.sparql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            # s = inner_res["s"]["value"]
            o = inner_res["o"]["value"]
            o_list.append(o)

        if PROFILER:
            t1 = time.time()
            total_time = t1 - t0
            self.timers['update_so_dict'] += total_time
        return o_list

    def __get_top_15_props(self, ps, n=5):
        p_dict_ret = {}
        for i, p in enumerate(ps):
            cur = ps[p]
            p_dict_ret[p] = int(cur)
            if i > n:
                m = min(p_dict_ret, key=p_dict_ret.get)
                p_dict_ret.pop(m, None)
        return p_dict_ret


    def get_p_dict_from_dump(self,quick, dump_name):
        p_dict_file = open(dump_name, 'r')
        p_dict = pickle.load(p_dict_file)
        p_dict_file.close()

        if quick:
            return self.__get_top_15_props(p_dict, n=5)
        else:
            return self.__get_top_15_props(p_dict, n=25)



    def get_s_dict_from_dump(self, quick, dump_name):

        s_dict_file = open(dump_name, 'r')
        s_dict = pickle.load(s_dict_file)

        s_dict_file.close()
        if quick:
            return self.__get_top_15_props(s_dict, n=50)
        else:
            return self.__get_top_15_props(s_dict, n=3000)


def mine_all_rules(dbt, st, surit, Q=False):
    print "**************started mining rules for:{} **********************".format(st)
    mm = miner(dbt, st, surit)
    mm.mine_rules(Q, min_pos_th=0.2, positive_total_ratio_th=0.85)
    GG = mm.RG
    dump_name = st + "/" + st + "_pg.dump"
    g_file = open(dump_name, 'w')
    pickle.dump(GG, g_file)
    g_file.close()

    dump_name = st + "/" + st + "_p_rels.dump"
    p_file = open(dump_name, 'w')
    pickle.dump(mm.p_rels, p_file)
    p_file.close()

    dump_name = st + "/" + st + "_miner.dump"
    m_file = open(dump_name, 'w')
    pickle.dump(mm, m_file)
    m_file.close()
    print "=================finished mining rules for:{} ======================".format(st)


if __name__ == '__main__':
    # from find_inconsistecies import fix_graphic
    DEBUG = True
    quick = True
    db = DBPEDIA_URL_UP

    for d in [{'comedian': "http://dbpedia.org/ontology/Comedian"}]:

    #for d in dictionariesq:
        for s, suri in d.items():
            # t = Thread(target=mine_all_rules, args=(DBPEDIA_URL, s, suri, quick,))
            # t.start()
            mine_all_rules(DBPEDIA_URL_UP, s, suri, quick)



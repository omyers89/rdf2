from SPARQLWrapper import SPARQLWrapper, JSON

import pickle
import sys
import os
import time
import graphp
from threading import Thread
from Utils import *
import re
DBPEDIA_URL = "http://tdk3.csf.technion.ac.il:8890/sparql"
DBPEDIA_URL_UP = "http://dbpedia.org/sparql"
SMAL_URL = "http://cultura.linkeddata.es/sparql"
DEBUG = False
PROFILER = True




class DbpKiller():

    def __init__(self, kb, subj, s_uri):
        self.knowledge_base = kb
        self.subject = subj
        self.subject_uri = s_uri
        self.sparql = SPARQLWrapper(kb)
        self.RG = graphp.SubjectGraph(s_uri)
        self.timers = {'get_os': 0, 'update_so_dict': 0}

    def __get_top_15_props(self, ps, n=5):
        p_dict_ret = {}
        for i, p in enumerate(ps):
            cur = ps[p]
            p_dict_ret[p] = int(cur)
            if i > n:
                m = min(p_dict_ret, key=p_dict_ret.get)
                p_dict_ret.pop(m, None)
        return p_dict_ret


    def get_s_dict_from_dump(self, quick, dump_name):

        s_dict_file = open(dump_name, 'r')
        s_dict = pickle.load(s_dict_file)

        s_dict_file.close()
        if quick:
            return self.__get_top_15_props(s_dict, n=50)
        else:
            return self.__get_top_15_props(s_dict, n=3000)


    def get_po_dict(self, s):

        if PROFILER:
            t0 = time.time()
        po_dict = {}
        query_text = ("""
                    SELECT DISTINCT ?p ?o
                    WHERE{
                            <%s> ?p ?o .
                            ?o a ?t.
                            FILTER (regex(?p, "^http://dbpedia.org/property/", "i") || regex(?p, "^http://dbpedia.org/ontology/", "i"))
                        } """ % (s))

        self.sparql.setQuery(query_text)
        self.sparql.setReturnFormat(JSON)
        results_inner = self.sparql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            # s = inner_res["s"]["value"]
            o = inner_res["o"]["value"]
            p = inner_res["p"]["value"]
            if p not in po_dict:
                po_dict[p]=[]
            po_dict[p].append(o)

        if PROFILER:
            t1 = time.time()
            total_time = t1 - t0
            self.timers['update_so_dict'] += total_time
        return po_dict


    def get_sim(self, p1,p2):
        tot_sim = 0
        if len(p1) <= len(p2):
            for o in p1:
                if o in p2:
                    tot_sim+=1
            return float(tot_sim) / len(p1)
        else:
            for o in p2:
                if o in p1:
                    tot_sim += 1
            return float(tot_sim) / len(p2)


    def get_op_sim_dict(self, sim_res_rules):
        op_sim = {}
        for (k1, k2) in sim_res_rules:
            matchObj = re.search(r'ontology', k1 , flags=0)
            if matchObj:
                if k1 not in op_sim:
                    op_sim[k1] = {}
                op_sim[k1][k2] = True
        return  op_sim


    def kill_dbp(self, quick, sim_th=0.6, tot_retio=0.5):
        if PROFILER:
            t0 = time.time()
        print "mining rules for {}".format(self.subject)
        s_dump_name = self.subject + "/" + self.subject + "_top.dump"
        #p_dump_name = self.subject + "/" + self.subject + "_prop_p.dump"
        #o_dump_name = self.subject + "/" + self.subject + "_prop.dump"
        # get the 100 most popular properties for type person in dbp
        #p_dict = self.get_p_dict_from_dump(quick, p_dump_name)
        s_dict = self.get_s_dict_from_dump(quick, s_dump_name)
        #o_dict = self.get_p_dict_from_dump(quick, o_dump_name)

        #p_dump_name = self.subject + "/" + self.subject + "_prop.dump"
        # get the 100 most popular properties for type person in dbp

        sim_tup_dict = {}
        sim_res_rules = {}
        for s in s_dict:
            p_o_dict = self.get_po_dict(s)
            l1 = p_o_dict.items()
            l2 = p_o_dict.items()
            for i in range(0, len(l1) -1):
                for j in range(i+1, len(l2) -1):
                    p1 = l1[i]
                    p2 = l2[j]
                    sim = self.get_sim(p1[1],p2[1])
                    if p1[0] < p2[0]:
                        if (p1[0], p2[0]) not in sim_tup_dict:
                            sim_tup_dict[(p1[0], p2[0])] = {'tot': 0, 'sim': 0}
                            #sim_tup_dict[(p1[0], p2[0])] = 0
                        sim_tup_dict[(p1[0], p2[0])]['tot'] += 1
                        if sim > sim_th:
                            sim_tup_dict[(p1[0], p2[0])]['sim'] += 1
                    else:
                        if (p2[0], p1[0]) not in sim_tup_dict:
                            sim_tup_dict[(p2[0], p1[0])] = {'tot': 0, 'sim': 0}
                            #sim_tup_dict[(p2[0], p1[0])]['sim'] = 0
                        sim_tup_dict[(p2[0], p1[0])]['tot'] += 1
                        if sim > sim_th:
                            sim_tup_dict[(p2[0], p1[0])]['sim'] += 1

        for ps, counts in sim_tup_dict.items():
            if float(counts['tot'])/len(s_dict) > 0.1:
                if float(counts['sim'])/counts['tot'] > tot_retio:
                    sim_res_rules[ps] = counts
        if DEBUG:
            print "*****printing dict for ratio:" + str(sim_th) + "; " + str(tot_retio) + "*****"
            for k,v in sim_res_rules.items():
                print k, v

        op_sim_dict = self.get_op_sim_dict(sim_res_rules)

        dir_name = self.subject
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        dump_name = dir_name + "/" + self.subject + "_f_rules.dump"
        r_dict_file = open(dump_name, 'w')
        pickle.dump((sim_res_rules, op_sim_dict), r_dict_file)
        r_dict_file.close()

        if PROFILER:
            t1 = time.time()
            total_time = t1 - t0
            self.timers['update_so_dict'] += total_time
            print self.timers
        return (sim_res_rules, op_sim_dict)



    def check_rel_path(self, obj1, obj2):


        query = ("""
        PREFIX dbr: <http://dbpedia.org/resource/>
        SELECT (COUNT(DISTINCT ?o1) as ?cnt)
                        WHERE{
                   OPTIONAL {
                            <%s> ?p1 ?o1.
                              ?o1 a ?t1.
                  <%s> ?p11 ?o1.
                  }
               OPTIONAL {
                            <%s> ?p1 ?o1.
                              ?o1 a ?t1.
                   ?o1 ?p11 <%s>.
                  }
              OPTIONAL {
                            ?o1 ?p1  <%s> .
                              ?o1 a ?t1.
                  <%s> ?p11 ?o1.
                  }
               OPTIONAL {
                            ?o1 ?p1  <%s> .
                              ?o1 a ?t1.
                   ?o1 ?p11 <%s>.
                  }
          FILTER (! regex(?p1, "type", "i")).
          FILTER (! regex(?p11, "type", "i")).
          FILTER ( ?p1 > ?p11).

        }
          """% (obj1, obj2, obj1, obj2,obj1, obj2, obj1, obj2))


def find_feature_incs(s, suri, th=0.8, tut=0.7, quick=False):
    dk = DbpKiller(DBPEDIA_URL_UP, s, suri)
    dk.kill_dbp(quick,th, tut)


if __name__ == '__main__':
    DEBUG = True

    find_feature_incs([{'comedian': "http://dbpedia.org/ontology/Comedian"}], 0.8, 0.7, True)



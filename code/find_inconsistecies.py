import pickle
from SPARQLWrapper import SPARQLWrapper, JSON
import sys
import os
from miner import miner
from graphp import evaluate_selection
from Utils import *
from feature import *
DBPEDIA_URL = "http://dbpedia.org/sparql"

try_rules = [{'p': "http://dbpedia.org/ontology/residence" ,'t':	"http://dbpedia.org/resource/City"},
             {'p': "http://dbpedia.org/ontology/birthPlace", 't':"http://dbpedia.org/resource/City"}]

DEBUG = False

def check_rel(t, s_uri, p, G):
    inner_g = G.graph
    tp = t + '@' + p
    sparql = SPARQLWrapper(DBPEDIA_URL)
    query_text = ("""
        SELECT distinct  ?r12 ?r21
        WHERE {
                <%s> <%s> ?o1.
                <%s> <%s> ?o2.


               FILTER (?o1 < ?o2).
                        ?o1 a <%s>.
                        ?o2 a <%s>.

                        OPTIONAL {
                        ?o1 ?r12 ?o2.
                        ?o2 ?r21 ?o1.
                        }

                        FILTER regex(?r12, "^http://dbpedia.org/ontology", "i").
                        FILTER regex(?r21, "^http://dbpedia.org/ontology", "i").


        }""" % ( s_uri, p, s_uri, p,t,t))
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    for inner_res in results_inner["results"]["bindings"]:
        #o = inner_res["o"]["value"]
        r12 = inner_res["r12"]["value"]
        r21 = inner_res["r21"]["value"]
        if inner_g.has_edge(tp,tp,r12) and inner_g.has_edge(tp,tp,r21):
            return max(inner_g[tp][tp][r12]['support'],inner_g[tp][tp][r21]['support'])
        elif inner_g.has_edge(tp,tp,r12):
            return inner_g[tp][tp][r12]['support']
        elif inner_g.has_edge(tp,tp,r21):
            return inner_g[tp][tp][r21]['support']
        else:
            return -1

def check_p_rel(t, s_uri, p, ps_rels):

    sparql = SPARQLWrapper(DBPEDIA_URL)
    query_text = ("""
            SELECT distinct  ?r12 ?r21
            WHERE {
                    <%s> <%s> ?o1.
                    <%s> <%s> ?o2.
                   FILTER (?o1 < ?o2).
                            ?o1 a <%s>.
                            ?o2 a <%s>.

                            OPTIONAL {
                            ?o1 ?r12 ?o2.
                            ?o2 ?r21 ?o1.
                            }

                            FILTER regex(?r12, "^http://dbpedia.org/ontology", "i").
                            FILTER regex(?r21, "^http://dbpedia.org/ontology", "i").


            }""" % (s_uri, p, s_uri, p, t, t))
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    for inner_res in results_inner["results"]["bindings"]:
        r12 = inner_res["r12"]["value"]
        r21 = inner_res["r21"]["value"]
        r12_support = 0
        r21_support = 0
        if r12 in ps_rels[p]:
            r12_support = ps_rels[p][r12]
        if r21 in ps_rels[p]:
            r21_support = ps_rels[p][r21]

        return max(r12_support,r21_support)


def fix_dbpedia(db, rules, s_uri, subj, load=True):
    rf_name = subj + "/" + subj + "_rules.dump"
    #rg_name = subj + "/" + subj + "_pg.dump"
    rp_name = subj + "/" + subj + "_p_rels.dump"

    if not os.path.exists(rf_name) or not os.path.exists(rp_name):
        return

    ons = {}
    sparql = SPARQLWrapper(db)
    if load:
        rules_file = open(rf_name, 'r')
        all_rules = pickle.load(rules_file)
        #all_rules_list = (rules70_, rules60_70, rules70_dbo, rules_wierd, rules_wierd_dbo, one_of_a_kind, low_props)
        (rules, r_67, r_7_dbo, wrd, wrd_dbo, ons, lows) = all_rules
        rules_file.close()

        g_file = open(rp_name, 'r')
        ps_rels = pickle.load(g_file)
        g_file.close()

    print "find inconsistencies, number of rules: {} ".format(str(len(rules)))
    i = 0
    inco_dict = {}
    inco_ons ={}
    inco_dbot_dict = {}
    #for d, rn in [(rules, '85' ), (r_67, '67')]:
    for d, rn in [(rules, '85' )]:
        for key, r in d.items():
            i+=1
            p = r['p']
            t = r['t']
            # count entities that ruin uniqueness
            # for every property and type we mined before count and find the
            # violations.
            query_text = ("""
                SELECT ?s ?cnt
                WHERE {
                {
                    SELECT ?s (COUNT(*) AS ?cnt)
                    WHERE{
                        ?o a <%s>.
                        ?s a <%s>;
                         <%s> ?o .

                    }GROUP BY ?s
                    ORDER BY DESC(?cnt)
                }
                FILTER ((?cnt > 1) && (?cnt < 3))
                }""" % (t, s_uri, p))
            sparql.setQuery(query_text)
            sparql.setReturnFormat(JSON)
            results_inner = sparql.query().convert()
            for inner_res in results_inner["results"]["bindings"]:
                s = inner_res["s"]["value"]

                #rel_rate = check_rel(t, s, p, G)
                p_rel_rate = check_p_rel(t, s, p, ps_rels)
                if s not in inco_dict:
                    inco_dict[s] = []
                inco_dict[s].append((p, t, rn, p_rel_rate))

    for p in ons:
        query_text = ("""
            SELECT * 
            WHERE {
            {
                SELECT ?s (COUNT(*) AS ?cnt)
                WHERE {
                {
                    SELECT DISTINCT ?s ?o
                    WHERE{
                        ?s a <%s>;
                        <%s> ?o .
                        ?o a ?t.
                    }                  
                }
                }GROUP BY ?s
            }FILTER (?cnt > 1)
            }""" % (s_uri, p))
        sparql.setQuery(query_text)
        sparql.setReturnFormat(JSON)
        results_inner = sparql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            so = inner_res["s"]["value"]

            if so not in inco_ons:
                inco_ons[so] = []
            inco_ons[so].append((p, "***ons***","***ons***"))

    for d, rn in [(r_7_dbo,'dbot_rules')]:
        for key, r in d.items():
            i += 1
            p = r['p']
            t = r['t']
            # count entities that ruin uniqueness
            # for every property and type we mined before count and find the
            # violations.
            query_text = ("""
                SELECT ?s ?cnt
                WHERE {
                {
                    SELECT ?s (COUNT(*) AS ?cnt)
                    WHERE{
                        ?o <http://dbpedia.org/ontology/type> <%s>.
                        ?s a <%s>;
                         <%s> ?o .

                    }GROUP BY ?s
                    ORDER BY DESC(?cnt)
                }
                FILTER ((?cnt > 1) && (?cnt < 3))
                }""" % (t, s_uri, p))
            sparql.setQuery(query_text)
            sparql.setReturnFormat(JSON)
            results_inner = sparql.query().convert()
            for inner_res in results_inner["results"]["bindings"]:
                s = inner_res["s"]["value"]

                #rel_rate = check_rel(t, s, p, G)
                p_rel_rate = check_p_rel(t, s, p, ps_rels)
                if s not in inco_dbot_dict:
                    inco_dbot_dict[s] = []
                inco_dbot_dict[s].append((p, t, rn, p_rel_rate ))

    if not os.path.exists(subj):
        os.makedirs(subj)
    incos = (inco_dict, inco_ons, inco_dbot_dict)
    dump_name = subj + "_incs.dump"
    inc_file = open(subj + "/" + dump_name, 'w')

    pickle.dump(incos, inc_file)
    inc_file.close()
    return incos

def get_subjects(uri, i):
    sparql = SPARQLWrapper(DBPEDIA_URL)
    top_s_dict = {}
    limit = 9999
    offset = i * limit
    slimit = str(limit)
    soffset = str(offset)
    query_text = ("""
                    SELECT DISTINCT ?s
                       WHERE
                       {
                           ?s a <%s>.
                       } LIMIT %s
                       OFFSET %s

                   """ % (uri, slimit, soffset))

    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    all_dict = results_inner["results"]["bindings"]
    for inner_res in all_dict:
        s = inner_res["s"]["value"]
        top_s_dict[s] = {}
    if len(all_dict) > 10:
        return top_s_dict ,True
    return top_s_dict, False



def fix_graphic(db, r_graph, s_uri, subj, fast=True, load = False):

    if load:
        r_graph = rules_dict_from_dump(subj + '/' + subj + '_pg.dump')

    mm = miner(db,subj, s_uri)
    #TODO: get the props from selected miner
    p_dump_name = subj + "/" + subj + "_prop.dump"
    # get the 100 most popular properties for type person in dbp
    ps = mm.get_p_dict_from_dump(fast, p_dump_name)

    i = 0
    cont = True
    ranks = {}
    while cont:
        subs, cont = get_subjects(s_uri, i)
        i+=1
        for s in subs:
            sg = mm.get_sub_graph( s, ps, fast)
            diff_evaluation = evaluate_selection(r_graph, sg)
            ranks[s] = diff_evaluation
        if fast:
            cont = i < 5
    if not os.path.exists(subj):
        os.makedirs(subj)
    dump_name = subj + "_Gincs.dump"
    inc_file = open(subj + "/" + dump_name, 'w')
    pickle.dump(ranks, inc_file)
    inc_file.close()




def find_p_incs(DBPEDIA_URL, s, suri, all_incs, fast=False):
    rf_name = s + "/" + s + "_f_rules.dump"
    #rg_name = subj + "/" + subj + "_pg.dump"
    if not os.path.exists(rf_name):
        return
    fet= DbpKiller(DBPEDIA_URL, s, suri)
    ons = {}
    sparql = SPARQLWrapper(DBPEDIA_URL)

    rules_file = open(rf_name, 'r')
    all_p_rules_tup  = pickle.load(rules_file)
    all_p_rules, op_sim_dict = all_p_rules_tup
    rules_file.close()

    print "find inconsistencies PS, number of rules: {} ".format(str(len(all_p_rules)))
    i = 0

    cont = True
    pincs = {}
    while cont:
        subs, cont = get_subjects(suri, i)
        i += 1
        if fast:
            if i==1:
                cont = False
        for j, su in enumerate(subs):
            if su in all_incs[0] or su in all_incs[1] or su in all_incs[2]:
                p_o_dict = fet.get_po_dict(su)
                #l1 = p_o_dict.items()
                for d in all_p_rules:
                    p1 = d[0]
                    p2 = d[1]
                    if p1 in p_o_dict and p2 in p_o_dict:
                        sim = fet.get_sim(p_o_dict[p1], p_o_dict[p2])
                        if sim < 0.5:
                            if su not in pincs:
                                pincs[su] = []
                            pincs[su].append((p1,p2))

                if DEBUG:
                    txt = "\b f inc progress:{}/{}".format(j,i)
                    sys.stdout.write(txt)
                    sys.stdout.write("\r")
                    sys.stdout.flush()

    dump_name = s + "_p_incs.dump"
    inc_file = open(s + "/" + dump_name, 'w')
    pickle.dump(pincs, inc_file)
    inc_file.close()
    return pincs



def rules_dict_from_dump(dump_name):
        r_graph_file = open(dump_name, 'r')
        p_dict = pickle.load(r_graph_file)
        r_graph_file.close()
        return p_dict



def find_all_incs( s, suri, fast=False):
    rules = {}
    incs = fix_dbpedia(DBPEDIA_URL, rules, suri, s, load=True)
    find_p_incs(DBPEDIA_URL, s, suri, incs, fast)
    # fix_graphic(DBPEDIA_URL, rules, suri, s,fast=True, load=True)


def find_only_p_incs(s, suri, fast=False):
    #rules = {}
    dump_name = s + "/" + s + "_incs.dump"
    incs = rules_dict_from_dump(dump_name)
    find_p_incs(DBPEDIA_URL, s, suri, incs, fast)
    # fix_graphic(DBPEDIA_URL, rules, suri, s,fast=True, load=True)

if __name__ == '__main__':

    for d in [{'comedian': "http://dbpedia.org/ontology/Comedian"}]:

        # for d in dictionariesq:
        for s, suri in d.items():
            # t = Thread(target=mine_all_rules, args=(DBPEDIA_URL, s, suri, quick,))
            # t.start()
            find_all_incs( s, suri, True)

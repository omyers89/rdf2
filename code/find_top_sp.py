from SPARQLWrapper import SPARQLWrapper, JSON
import os
import pickle
import sys
from threading import Thread
from Utils import *

DBPEDIA_URL = "http://dbpedia.org/sparql"


def get_top_1_percent(i, top_s_dict,uri, f_limit = 200):
    sparql = SPARQLWrapper(DBPEDIA_URL)

    limit = 10000
    offset = i * limit
    s_f_limit = str(f_limit)

    slimit = str(limit)
    soffset = str(offset)
    query_text = ("""
    SELECT ?s(COUNT(*)AS ?scnt)
    WHERE
    {
        {
            SELECT DISTINCT ?s ?p
            WHERE
            {
                {
                    SELECT DISTINCT ?s
                    WHERE
                    {
                        ?s a <%s>.
                    } LIMIT %s
                    OFFSET %s
                }
                ?s ?p ?o.
                FILTER regex(?p, "^http://dbpedia.org/ontology/", "i")
            }
        }
    } GROUP BY ?s
    ORDER BY DESC(?scnt)
    LIMIT %s""" % (uri,slimit, soffset, s_f_limit))

    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    all_dict = results_inner["results"]["bindings"]
    for inner_res in all_dict:
        s = inner_res["s"]["value"]
        cnt = inner_res["scnt"]["value"]
        if cnt>20:
            top_s_dict[s] = cnt
    if len(all_dict) > 10:
        return True
    return False


def get_f_limits(uri):
    cnt = 1
    sparql = SPARQLWrapper(DBPEDIA_URL)
    query_text = ("""
        SELECT (COUNT(*)AS ?scnt)
        WHERE
        {
            {
                SELECT DISTINCT ?s
                WHERE
                {
                    ?s a <%s>.
                }
            }
        }
        """ % (uri))

    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    all_dict = results_inner["results"]["bindings"]
    for inner_res in all_dict:
        cnt = inner_res["scnt"]["value"]

    r = float(5000)/ float(cnt)

    l = r * 10000 if r < 0.5 else int(cnt)

    # just to make sure we dont miss anyone
    return int(l), int(cnt)



def get_all_top_of(uri , f_name, dir_name):

    i=0
    top_subjects = {}
    limits, maxs = get_f_limits(uri)
    flag = get_top_1_percent(i, top_subjects, uri, limits)
    while flag:
        i += 1
        flag = get_top_1_percent(i, top_subjects, uri, limits)

        # txt = "\b i progress:{} ".format(i)
        # sys.stdout.write(txt)
        # sys.stdout.write("\r")
        # sys.stdout.flush()
        if i>150 or i*1000>maxs:
            flag = False

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    s_dict_file = open(dir_name + "/" + f_name, 'w')
    pickle.dump(top_subjects, s_dict_file)
    s_dict_file.close()
    #print "get top s  done for {}, i is:{}".format(f_name, i)



def get_all_p_dict(uri, dump_name,dir_name):
    sparql = SPARQLWrapper(DBPEDIA_URL)
    p_dict = {}


    query_text = ("""
            SELECT ?p (COUNT (?p) AS ?cnt)
            WHERE {
                    {
                    SELECT DISTINCT ?s ?p
                    WHERE {
                        ?s a <%s>;
                            ?p ?o.
                        ?o a ?t
                    FILTER regex(?p, "^http://dbpedia.org/ontology/", "i")
                }LIMIT 500000
            }
            }GROUP BY ?p
             ORDER BY DESC(?cnt)
             LIMIT 50
            """ % uri)
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()

    for inner_res in results_inner["results"]["bindings"]:
        p = inner_res["p"]["value"]
        cnt = inner_res["cnt"]["value"]
        p_dict[p] = cnt

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    p_dict_file = open(dir_name + "/" + dump_name, 'w')
    pickle.dump(p_dict, p_dict_file)
    p_dict_file.close()
    #print "pdict done for: {}".format(dir_name)


def get_p_p_dict(uri, dump_name,dir_name):
    sparql = SPARQLWrapper(DBPEDIA_URL)
    p_dict = {}


    query_text = ("""
            SELECT ?p (COUNT (?p) AS ?cnt)
            WHERE {
                    {
                    SELECT DISTINCT ?s ?p
                    WHERE {
                        ?s a <%s>;
                            ?p ?o.
                        ?o a ?t
                    FILTER regex(?p, "^http://dbpedia.org/property/", "i")
                }LIMIT 500000
            }
            }GROUP BY ?p
             ORDER BY DESC(?cnt)
             LIMIT 50
            """ % uri)
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()

    for inner_res in results_inner["results"]["bindings"]:
        p = inner_res["p"]["value"]
        cnt = inner_res["cnt"]["value"]
        p_dict[p] = cnt

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    p_dict_file = open(dir_name + "/" + dump_name, 'w')
    pickle.dump(p_dict, p_dict_file)
    p_dict_file.close()
    #print "pdict done for: {}".format(dir_name)


def get_ps(uri, s_name ):
    print "started: " +s_name
    subjects_fname = s_name + "_top.dump"
    pprop_fname = s_name + "_prop.dump"
    pprop_fname_p = s_name + "_prop_p.dump"

    get_all_top_of(uri, subjects_fname, s_name)
    get_all_p_dict(uri, pprop_fname, s_name)
    #get_p_p_dict(uri, pprop_fname_p, s_name)

    print "finished: " + s_name



if __name__ == '__main__':

    for d in dictionariest:
        for s, uri in d.items():
            t = Thread(target=get_ps, args=(uri,s,))
            t.start()













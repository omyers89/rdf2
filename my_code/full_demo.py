from miner import *
from Utils import *
from find_inconsistecies import find_all_incs, find_only_p_incs
from get_rules import get_all_rules
from get_incs import  get_all_incs
from feature import find_feature_incs
from find_top_sp import get_ps
from get_s_dict import get_subj_from_dump
from e_sender import send_email


DBPEDIA_URL = "http://tdk3.csf.technion.ac.il:8890/sparql"
DBPEDIA_URL_UP = "http://dbpedia.org/sparql"


def mine_rules_find_incs(s, suri, quick):
    stage = ""
    try:
        get_ps(suri, s)
        get_subj_from_dump(s)
        stage= "before"
        print stage , s
        mine_all_rules(DBPEDIA_URL, s, suri, quick)
        stage = "after allrules"
        print stage , s
        get_all_rules(s)
        stage = " after get all rules"
        print stage , s
        find_feature_incs(s, suri, th=0.8, tut=0.7, quick=False)
        stage = " after find p incs"
        find_all_incs(s, suri, fast=quick)

        # find_only_p_incs(s, suri, fast=quick)
        stage = "after find all incs"
        print stage , s
        get_all_incs(s)
        stage = "after get all incs"
        print stage , s
    except Exception as e:
        msg = "went off at s:{}, stage:{}, err is:{} ".format(s, stage,e)
        print msg
        send_email("error at full demo", msg)



if __name__ == '__main__':
    quick = False

    db = DBPEDIA_URL


    #min_pos_th = 0 #a small number just to separate the bizzar rule as we will see in the example.
   # positive_total_ratio_th = 0.82  #selected after trying few values found to be the most suitable for the ration between
    #positive to total appearence where the positive is when tuple (p,t ) is unique

    #for d in dictionariest:
    threads = []
    for d in dictionariest:
        for s, suri in d.items():
            #t = Thread(target=mine_rules_find_incs, args=(s, suri, quick,))
            #threads.append(t)
            #t.start()
            mine_rules_find_incs(s, suri, quick,)
    
    #for trd in threads:
     #   trd.join()
    
    msg = "finished full demo "
    print msg
    send_email("success", msg)
                


    # for s, suri in subjects0.items():
    #
    #     all_rules = mine_rules(db, quick, s, min_pos_th, positive_total_ratio_th)
    #     #rf_name = "rules/" + s + "_rules.dump"
    #     #rules_file = open(rf_name, 'w')
    #     #pickle.dump(all_rules, rules_file)
    #     #rules_file.close()
    #
    #     # incs_name = "incs/" + s + "_incs.dump"
    #     # rules, r60_70, r50_60, dumy = all_rules
    #     # fix_dbpedia(db, rules, suri,incs_name, False)
    #

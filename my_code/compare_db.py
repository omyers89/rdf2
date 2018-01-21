from miner_base import *


DBPEDIA_URL = "http://tdk3.csf.technion.ac.il:8890/sparql"
DBPEDIA_URL_UP = "http://dbpedia.org/sparql"


QUICK = False
"""
the idea of this module is to find the properties that in new versions of DBpedia has objects related to them that are
not exist in the previous sets.

"""
def get_spo_dict(kb):
    """
    This function get a kb and return a
    dictionary of all s,p and objects related to them.
    :param kb: DBpedia endpoint url.
    :return: dictionary {(s,p):{o1,o2,o3}}
    """
    miner = MinerBase(kb)
    s_dict = miner.get_s_dict_from_dump(quick=QUICK, dump_name="../results/person/FINAL/person_top.dump")
    p_dict = miner.get_p_dict_from_dump(quick=QUICK, dump_name="../results/person/FINAL/person_200_prop_for_ML.dump", nx=200)
    res_dict={}
    for s in  s_dict:
        for p in p_dict:
            obj_list = miner.get_objects_for_s_p(p,s)
            if len(obj_list)>0:
                res_dict[(s,p)] = obj_list


    return res_dict



def comp_db():
    now_res = get_spo_dict(DBPEDIA_URL_UP)
    past_res = get_spo_dict(DBPEDIA_URL)
    #http://client.linkeddatafragments.org/#datasources=http%3A%2F%2Ffragments.dbpedia.org%2F2016-04%2Fen
    # pastt = get_spo_dict("http://client.linkeddatafragments.org/#datasources=http%3A%2F%2Ffragments.dbpedia.org%2F2016-04%2Fen")
    prop_dif_dict = {}
    for kn, onl in now_res.items():
        prop = kn[1]
        if prop not in prop_dif_dict:
            prop_dif_dict[prop] = {"diff": 0, "tot": 0, "past_miss": 0}
        prop_dif_dict[prop]["tot"] += 1
        if kn in past_res:
            ool = past_res[kn] #list of old objects
            for on in onl:
                if on not in ool:
                    prop_dif_dict[prop]["diff"] += 1
        else:
            prop_dif_dict[prop]["past_miss"] += 1



    dir_name = "person"
    dump_name = dir_name + "_diff_props.dump"
    dir_name = "../results/" + dir_name
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    p_dict_file = open(dir_name + "/FINAL/" + dump_name, 'w')
    pickle.dump(prop_dif_dict, p_dict_file)
    p_dict_file.close()

if __name__ == "__main__":
    comp_db()


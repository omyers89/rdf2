import csv
import pickle
import os
from Utils import dictionaries, dictionariest


def get_incs_f(subj_name):
    rf_name = "../results/" + subj_name + "/" + subj_name + "_incs.dump"
    if not os.path.exists(rf_name):
        return
    incs_file = open(rf_name, 'r')
    incos = pickle.load(incs_file)
    if len(incos) < 3: return
    (inco_dict, inco_ones, inco_dbot_dict) = incos
    incs_file.close()
    csvf_name = "../results/" + subj_name + "/" + subj_name + "_incs.csv"
    csvf_dbo_name = "../results/" + subj_name + "/" + subj_name + "_incs_dbot.csv"
    for (inc_dict, inc_name) in [(inco_dict, csvf_name), (inco_dbot_dict, csvf_dbo_name)]:
        with open(inc_name, 'w') as csvfile:
            fieldnames = ['Person', 'Property', 'Type', 'rn', 'rel_rate']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for pers, pt in inc_dict.items():
                uni_pers = pers.encode('utf-8')
                for p, t, rn , rl in pt:
                    uni_p = p.encode('utf-8')
                    p_list = str(uni_p).rsplit('/')
                    p_short = p_list[len(p_list)-1]
                    uni_t = t.encode('utf-8')
                    t_list = str(uni_t).rsplit('/')
                    t_short = t_list[len(t_list) - 1]
                    if not rl == None:
                        gr = 1 - rl
                    else:
                        gr = "**"
                    writer.writerow(
                        {'Person': uni_pers, 'Property': p_short, 'Type': t_short, 'rn': rn, 'rel_rate': str(gr)})
                    #print {'Person': pers, 'Property': p, 'Type': t}
        csvfile.close()

    csvf_name = "../results/" + subj_name + "/" + subj_name + "_ons_incs.csv"
    with open(csvf_name, 'w') as csvfile:
        fieldnames = ['Person', 'Property', 'Type', 'rn']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for pers, pt in inco_ones.items():
            uni_pers = pers.encode('utf-8')
            for p, t, rn in pt:
                uni_p = p.encode('utf-8')
                uni_t = t.encode('utf-8')
                writer.writerow({'Person': uni_pers, 'Property': uni_p, 'Type': uni_t, 'rn': rn})
                # print {'Person': pers, 'Property': p, 'Type': t}
    csvfile.close()



def get_incs_p(subj_name):
    rf_name = "../results/" + subj_name + "/" + subj_name + "_p_incs.dump"
    if not os.path.exists(rf_name):
        return
    incs_file = open(rf_name, 'r')
    incos = pickle.load(incs_file)

    incs_file.close()
    csvf_name = "../results/" + subj_name + "/" + subj_name + "_p_incs.csv"
    with open(csvf_name, 'w') as csvfile:
        fieldnames = ['subj','p1', 'p2']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for su, pt in incos.items():
            uni_su = su.encode('utf-8')
            for p1,p2 in pt:
                uni_p1 = p1.encode('utf-8')
                uni_p2 = p2.encode('utf-8')
                writer.writerow({'subj': uni_su, 'p1': uni_p1, 'p2': uni_p2})
                # print {'subj': uni_su, 'p1': uni_p1, 'p2': uni_p2}
    csvfile.close()



def get_all_incs(subj):
    get_incs_f(subj)
    get_incs_p(subj)

if __name__ == '__main__':

    for d in [{'comedian': "http://dbpedia.org/ontology/Comedian"}]:
        for s, suri in d.items():
            get_all_incs(s)




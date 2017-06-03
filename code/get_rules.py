import csv
import pickle
import os
from Utils import dictionaries, dictionariest



def print_rules_to_csv(subj):
   
    
    rf_name = subj + "/" + subj + "_rules.dump"
    if not os.path.exists(rf_name):
        return
    rules_file = open(rf_name, 'r')
    all_rules = pickle.load(rules_file)
    if len(all_rules) < 6: return
   # good, r60_70, r50_60, weird , ons, lows= all_rules
    rules_file.close()
    #all_rules_list = (rules70_, rules60_70, rules70_dbo, rules_wierd, rules_wierd_dbo, one_of_a_kind, low_props)
    csv_names = ['good.csv', 'r60_70.csv', 'rtop_dbot.csv', 'weird.csv', 'rules_wierd_dbo.csv',  'ons.csv', 'lows.csv']

    for rd, csvn in zip(all_rules, csv_names):
        csvf_name = subj + "/" + csvn
        with open(csvf_name, 'w') as csvfile1:
            fieldnames = ['Property', 'Type', 'Ratio', 'support']
            writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)

            writer.writeheader()
            if csvn == 'ons.csv':
                for r, rt in rd.items():
                    prop = r.encode('utf-8')
                    pos = float(rt)
                    data = {'Property': prop, 'Type': "", 'Ratio': pos, 'support': ""}
                    writer.writerow(data)
                continue
            else:
                for k, r in rd.items():
                    prop = (r['p']).encode('utf-8')
                    typet = (r['t']).encode('utf-8')
                    pos = float(r['pos'])
                    tot = float(r['tot'])
                    ratio = pos/tot
                    data = {'Property': prop, 'Type': typet, 'Ratio': ratio, 'support': tot}
                    writer.writerow(data)

        csvfile1.close()



def print_f_rules_to_csv(subj):


    rf_name = subj + "/" + subj + "_f_rules.dump"
    if not os.path.exists(rf_name):
        return
    rules_file = open(rf_name, 'r')
    all_p_rules_tup = pickle.load(rules_file)
    rules_file.close()

    all_p_rules, op_sim_dict = all_p_rules_tup;
    csvf_name = subj + "/" + subj + "_f_rules.csv"

    with open(csvf_name, 'w') as csvfile1:
        fieldnames = ['p1', 'p2', 'Ratio']
        writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)

        writer.writeheader()
        for (p1,p2), r in all_p_rules.items():
            p1_uni = (p1).encode('utf-8')
            p2_uni = (p2).encode('utf-8')

            sim = float(r['sim'])
            tot = float(r['tot'])

            data = {'p1': p1_uni, 'p2': p2_uni, 'Ratio': sim/tot}
            writer.writerow(data)

    csvfile1.close()


def get_all_rules(subj):
    print_rules_to_csv(subj)
    print_f_rules_to_csv(subj)


if __name__ == '__main__':

    #get_all_rules([{'comedian': "http://dbpedia.org/ontology/Comedian"}])
    for d in [{'comedian': "http://dbpedia.org/ontology/Comedian"}]:
        for s, suri in d.items():
            get_all_rules(s)
    #for d in dictionaries:

        

    # rules_file = open("rules.dump", 'r')
    # all_rules = pickle.load(rules_file)
    # good, r60_70, r50_60, weird = all_rules
    # rules_file.close()

    # with open('rules.csv', 'w') as csvfile1:
    #     fieldnames = ['Property', 'Type', 'Ratio', 'support']
    #     writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)

    #     writer.writeheader()
    #     for r in good:
    #         prop = (r['p']).encode('utf-8')
    #         typet = (r['t']).encode('utf-8')
    #         pos = float(r['pos'])
    #         tot = float(r['tot'])
    #         ratio = pos/tot
    #         data = {'Property': prop, 'Type': typet, 'Ratio': ratio, 'support': tot}
    #         writer.writerow(data)

    # csvfile1.close()

    # with open('r60_70.csv', 'w') as csvfile1:
    #     fieldnames = ['Property', 'Type', 'Ratio', 'support']
    #     writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)

    #     writer.writeheader()
    #     for r in r60_70:
    #         prop = (r['p']).encode('utf-8')
    #         typet = (r['t']).encode('utf-8')
    #         pos = float(r['pos'])
    #         tot = float(r['tot'])
    #         ratio = pos / tot
    #         data = {'Property': prop, 'Type': typet, 'Ratio': ratio, 'support': tot}
    #         writer.writerow(data)

    # csvfile1.close()

    # with open('r50_60.csv', 'w') as csvfile1:
    #     fieldnames = ['Property', 'Type', 'Ratio', 'support']
    #     writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)

    #     writer.writeheader()
    #     for r in r50_60:
    #         prop = (r['p']).encode('utf-8')
    #         typet = (r['t']).encode('utf-8')
    #         pos = float(r['pos'])
    #         tot = float(r['tot'])
    #         ratio = pos / tot
    #         data = {'Property': prop, 'Type': typet, 'Ratio': ratio, 'support': tot}
    #         writer.writerow(data)

    # csvfile1.close()

    # with open('rules_weird.csv', 'w') as csvfile2:
    #     fieldnames = ['Property', 'Type', 'Ratio', 'support']
    #     writer = csv.DictWriter(csvfile2, fieldnames=fieldnames)

    #     writer.writeheader()
    #     for r in weird:
    #         prop = r['p'].encode('utf-8')
    #         typet = r['t'].encode('utf-8')
    #         pos = float(r['pos'])
    #         tot = float(r['tot'])
    #         ratio = pos / tot
    #         data = {'Property': prop, 'Type': typet, 'Ratio': ratio, 'support': tot}
    #         writer.writerow(data)

    # csvfile2.close()




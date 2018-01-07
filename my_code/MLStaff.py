import pickle
import sys
import os
import time
import csv
#import sklearn
from sklearn.neighbors import KNeighborsClassifier
from feature_miner import *

def creat_traning_data(subj):
    #reader = csv.DictReader(open('../results/person/person_features-old.dump', 'rb'))
    reader = csv.DictReader(open('../results/person/person_props_clsfd.csv', 'rb'))
    classified_properies_dict = {}
    for line in reader:
        if line['class'] == '2':
            continue
        uri = line['uri']
        clsf = int(line['class'])
        seg = int(line['cnter'])
        classified_properies_dict[uri] = {'seg': seg, 'class': clsf, 'features': {}}
    dump_name = "../results/" + subj + "/" + subj + "_features.dump"
    feature_dict_file = open(dump_name, 'r')
    feature_dict = pickle.load(feature_dict_file)
    feature_dict_file.close()

    max_seg = 0
    for (p,v) in classified_properies_dict.items():
        if v['seg'] > max_seg:
            max_seg = v['seg']

    # add here weighted features.

    for (prop, features) in feature_dict.items():
        if prop in classified_properies_dict:
            classified_properies_dict[prop]['features'] = features
            t_seg = classified_properies_dict[prop]['seg']
            classified_properies_dict[prop]['seg'] = (float(t_seg)/max_seg) / len(classified_properies_dict)

    #now we have in classified_properies_dict all props classified and with the features

    #the features:
    x_list = []
    #the lables:
    y_list = []

    for (p, v) in classified_properies_dict.items():
        features = v['features']
        weight = v['seg']
        if 'p_only_one_counter' not in features:
            continue

        x1 = float(features['p_only_one_counter'])
        x2 = float(features['p_multy_objs_same_type_counter'])
        x3 = float(features['p_objs_unique_type_counter'])
        for i in range(weight):
            x_list.append([x1,x2,x3])
            y_list.append(int(v['class']))

    #now we have here x_list with lists of fetures ordered at same order as the lables
    neigh = KNeighborsClassifier(n_neighbors=3)
    neigh.fit(x_list, y_list)

    dir_name = "../dumps"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    dump_name = dir_name + "/" + "knn_clsfr.dump"
    clsf_file = open(dump_name, 'w')
    pickle.dump(neigh, clsf_file)
    clsf_file.close()
    return neigh


def get_classes_prob_for_new_x(prop_uri, clsfirx, FMx, quick, nx):
    try:
        features = FMx.get_fetures_for_prop(quick, prop_uri,nx)
    except:
        return [-1,-1]
    if 'p_only_one_counter' not in features:
        return
    x1 = float(features['p_only_one_counter'])
    x2 = float(features['p_multy_objs_same_type_counter'])
    x3 = float(features['p_objs_unique_type_counter'])
    x_list=[x1, x2, x3]
    print x_list
    # prediction = clsfir.predict_proba[[x_list]]
    # print 'prob for ' + prop_uri + 'is:' + prediction
    # return prediction
    return clsfirx.predict_proba([x_list])

def get_class_with_prob(prop_uri, quick=True, nx=-1):
    clsfir = creat_traning_data('person')
    FM = FeatureMiner(DBPEDIA_URL_UP, 'person', "http://dbpedia.org/ontology/Person")
    res_prod_list=[0,0]
    bool_result = False
    try:
        res_prod_list = get_classes_prob_for_new_x(prop_uri, clsfir, FM, quick, nx)
        print "res_prod_list is:"
        print res_prod_list
        real_prob = res_prod_list[0]
        bool_result = real_prob[1] > 0.5  # if 1 (prob for temporal greater than 0.5 ) res is true for display
    except:
        LOG(" Failed to find probs for p: %s" % prop_uri)

    return bool_result, (real_prob[1] if bool_result else real_prob[0])





if __name__ == "__main__":
    #clsfir = creat_traning_data('person')
    #FM = FeatureMiner(DBPEDIA_URL_UP, 'person', "http://dbpedia.org/ontology/Person")
    # x1_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/militaryRank',clsfir, FM, False)
    # x11_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/nominee', clsfir, FM, False)
    # x0_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/deputy', clsfir, FM, False)
    # x11_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/governor', clsfir, FM, False)
    # x111_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/lieutenant', clsfir, FM, False)
    # x1111_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/relation', clsfir, FM, False)
    # x11111_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/vicePresident', clsfir, FM, False)
    # #http://dbpedia.org/ontology/vicePresident


    (b,p ) = get_class_with_prob("http://dbpedia.org/ontology/birthPlace", False, 10)
    #x00_sanity_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/birthPlace', clsfir, FM, False)
    #http://dbpedia.org/ontology/birthPlace
    print str(b) + ", " + str(p)
    print "results:"
    print "x1_list:"
    # print x1_list
    print "x00_sanity_list:"
   # print x00_sanity_list
    print "x11_list:"
    # print x11_list
    # print "x111_list:"
    # print x111_list
    # print "x1111_list:"
    # print x1111_list
    # print "x11111_list:"
    # print x11111_list
    # print "x0_list:"
    # print x0_list


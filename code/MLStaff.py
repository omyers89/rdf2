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
        clsf = line['class']
        classified_properies_dict[uri] = {'class': clsf, 'features': {}}
    dump_name = "../results/" + subj + "/" + subj + "_features.dump"
    feature_dict_file = open(dump_name, 'r')
    feature_dict = pickle.load(feature_dict_file)
    feature_dict_file.close()

    for (prop, features) in feature_dict.items():
        if prop in classified_properies_dict:
            classified_properies_dict[prop]['features'] = features

    #now we have in classified_properies_dict all props classified and with the features

    #the features:
    x_list = []
    #the lables:
    y_list = []

    for (p, v) in classified_properies_dict.items():
        features = v['features']
        if 'p_only_one_counter' not in features:
            continue
        y_list.append(int(v['class']))
        x1 = float(features['p_only_one_counter'])
        x2 = float(features['p_multy_objs_same_type_counter'])
        x3 = float(features['p_objs_unique_type_counter'])
        x_list.append([x1,x2,x3])

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


def get_class_for_new_x(prop_uri, clsfirx, FMx, quick):
    features = FMx.get_fetures_for_prop(quick, prop_uri)
    if 'p_only_one_counter' not in features:
        return
    x1 = float(features['p_only_one_counter'])
    x2 = float(features['p_multy_objs_same_type_counter'])
    x3 = float(features['p_objs_unique_type_counter'])
    x_list=[x1, x2, x3]
    # prediction = clsfir.predict_proba[[x_list]]
    # print 'prob for ' + prop_uri + 'is:' + prediction
    # return prediction
    return clsfirx.predict_proba([x_list])

if __name__ == "__main__":
    clsfir = creat_traning_data('person')
    FM = FeatureMiner(DBPEDIA_URL_UP, 'politician', "http://dbpedia.org/ontology/Politician")
    x1_list = get_class_for_new_x('http://dbpedia.org/ontology/militaryRank',clsfir, FM, False)
    x11_list = get_class_for_new_x('http://dbpedia.org/ontology/nominee', clsfir, FM, False)
    x0_list = get_class_for_new_x('http://dbpedia.org/ontology/restingPlacePosition', clsfir, FM, False)
    x00_list = get_class_for_new_x('http://dbpedia.org/ontology/monarch', clsfir, FM, False)
    #http://dbpedia.org/ontology/monarch

    print "results:"
    print "x1_list:"
    print x1_list
    print "x11_list:"
    print x11_list
    print "x00_list:"
    print x00_list
    print "x0_list:"
    print x0_list


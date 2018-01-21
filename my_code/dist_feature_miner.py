from miner_base import *
from string import rsplit
from threading import Lock, Thread


DEBUG = True

class DistFeatureMiner(MinerBase):

    def __init__(self, kb, subj, s_uri):
        MinerBase.__init__(self,kb)
        self.subject = subj
        self.subject_uri = s_uri
        self.f_lock = Lock()
        self.p_count = 0
        self.p_only_one_counter = 0
        self.p_multy_objs_same_type_counter = 0
        self.p_objs_unique_type_counter = 0
        self.type_counters = {}

    def mine_features(self, quick):
        print "mining rules for {}".format(self.subject)
        s_dump_name = "../results/" + self.subject + "/FINAL/" + self.subject + "_top.dump"
        #p_dump_name = "../results/" + self.subject + "/" + self.subject + "_prop100.dump"
        p_p_dump_name = "../results/" + self.subject + "/FINAL/" + self.subject + "_200_prop_for_ML.dump"
        # get the 100 most popular properties for type person in dbp
        #p_dict = self.get_p_dict_from_dump(quick, p_dump_name)
        p_dict = self.get_p_dict_from_dump(quick, p_p_dump_name)
        s_dict = self.get_s_dict_from_dump(quick, s_dump_name)

        dir_name = "../results/" + self.subject
        dump_name = dir_name + "/FINAL/" + self.subject + "_features200.dump"
        if not os.path.exists(dump_name):
            feature_dictionary = {}
        else:
            r_dict_file = open(dump_name, 'r')
            feature_dictionary = pickle.load(r_dict_file)
            r_dict_file.close()


        progress = 0
        p_size = len(p_dict)
        p_indx = 0
        missed_ps = []
        for p in p_dict:
            #http://dbpedia.org/property/publisher
            if p in feature_dictionary:
                print p , " - skipped"
                continue
            try:
                feature_dictionary[p] = self.get_fetures_for_prop(quick, p, 2000)
            except:
                missed_ps.append(p)

            if DEBUG:
                sys.stdout.write("\b p #{} done".format(p_indx))
                sys.stdout.write("\r")
                sys.stdout.flush()
            p_indx += 1

            dir_name = "../results/" + self.subject
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            dump_name = dir_name + "/" + self.subject + "_features200.dump"
            r_dict_file = open(dump_name, 'w')
            pickle.dump(feature_dictionary, r_dict_file)
            r_dict_file.close()
        return feature_dictionary, missed_ps

    def get_subj_from_uri(self,uri_strin):
        tsubj = rsplit(uri_strin,"/")[-1]
        return tsubj

    def atomic_counter_inc(self,  p_count, p_multy_objs_same_type_counter, p_objs_unique_type_counter, p_only_one_counter):
        self.f_lock.acquire()
        self.p_count += p_count
        self.p_multy_objs_same_type_counter += p_multy_objs_same_type_counter
        self.p_objs_unique_type_counter += p_objs_unique_type_counter
        self.p_only_one_counter += p_only_one_counter
        self.f_lock.release()


    def get_fetures_for_prop(self, quick, prop_uri, nx=-1):
        print "mining features for {}".format(prop_uri)
        s_dump_name = "../results/" + self.subject + "/" + self.subject + "_top.dump"
        #p_dict = self.get_p_dict_from_dump(quick, p_dump_name)
        s_dict = self.get_s_dict_from_dump(quick, s_dump_name, nx)
        progress = 0
        #p_size = len(p_dict)

        self.p_count = 0
        self.p_only_one_counter = 0
        self.p_multy_objs_same_type_counter = 0
        self.p_objs_unique_type_counter = 0
        thread_dict = {}
        j=0
        for i, s in enumerate(s_dict):
            j+=1
            t = Thread(target=self.update_counter, args=(prop_uri, s,))
            thread_dict[i] = t
            t.start()

            if DEBUG:
                txt = "\b S loop progress: {}".format(i)
                sys.stdout.write(txt)
                sys.stdout.write("\r")
                sys.stdout.flush()
            if j > 40:
                for ih, th in thread_dict.items():
                    th.join()
                thread_dict = {}
                j = 0
                if DEBUG:
                    print "flushed:"
                    print self.p_count, ";", self.p_multy_objs_same_type_counter, ";", self.p_objs_unique_type_counter, ";", self.p_only_one_counter

        for ih, th in thread_dict.items():
            th.join()

        progress += 1
        if self.p_count > 0:
            feature_dict = {"p_only_one_counter": float(self.p_only_one_counter) / self.p_count,
                                 "p_multy_objs_same_type_counter": float(self.p_multy_objs_same_type_counter) / self.p_count,
                                 "p_objs_unique_type_counter": float(self.p_objs_unique_type_counter) / self.p_count}
        else:
            feature_dict = {"p_only_one_counter": -1,
                                            "p_multy_objs_same_type_counter": -1,
                                            "p_objs_unique_type_counter": -1}
        print "final"
        print self.p_count, ";",self.p_multy_objs_same_type_counter, ";",self.p_objs_unique_type_counter,";", self.p_only_one_counter
        return feature_dict

    def update_counter(self, prop_uri, s):
        p_count=0
        p_multy_objs_same_type_counter=0
        p_objs_unique_type_counter=0
        p_only_one_counter=0
        o_list = self.get_objects_for_s_p(prop_uri, s)
        if len(o_list) == 0:
            return
        if len(o_list) > 0:
            # means that there is at least one object related to the subject.
            p_count = 1
        obj_rdf_types_dict = self.get_rdf_types_for_o(o_list)
        # obj_dbo_types_dict = self.get_dbo_types_for_o(o_list)
        # t_dict_rel = self.get_ot_unique_dict_rel(o_list, obj_rdf_types_dict)  # Done: for specific person and property find the unique types!
        if len(o_list) > 1:
            # check if there is only one object of every type
            dbo_t_uniques = self.check_multiple_vals_same_type(o_list,
                                                               obj_rdf_types_dict)  # Done: for specific person and property find the unique types!

            if not dbo_t_uniques:
                p_multy_objs_same_type_counter = 1
            else:
                p_objs_unique_type_counter = 1

                # if not unique then there are multiple objects of the same type.

        # taking care of the first feature
        elif len(o_list) == 1:
            p_only_one_counter = 1
        # self.update_graph(s, p, rdf_t_dict) - move to end of s dict
        # self.update_graph_rel(p, o_list, obj_rdf_types_dict)
        self.atomic_counter_inc(p_count, p_multy_objs_same_type_counter, p_objs_unique_type_counter, p_only_one_counter)

    def check_multiple_vals_same_type(self, o_list, o_dict_t):
        res_dict = {}
        # single= False
        # if len(os) == 1:
        #     single = True
        for o in o_list:
            if o in o_dict_t:
                for t in o_dict_t[o]:
                    # if (t in res_dict) or single:
                    if t in res_dict:
                        res_dict[t] = False  # this is the second time t in res_dict so not unique!
                        return False
                    else:
                        res_dict[t] = True  # this is the first time t in res_dict so unique so far!
        return True


if __name__ == "__main__":
    # this script will mine all features of all properties of person
    FM = DistFeatureMiner(DBPEDIA_URL_UP, 'person', "http://dbpedia.org/ontology/Person")
    fd, missed = FM.mine_features(quick=False)
    if len(missed) > 0:
        # try again:
        fd, missed = FM.mine_features(quick=False)

    print "tried twice ps left:", missed
    # features = FM.get_fetures_for_prop(False, "http://dbpedia.org/ontology/spouse", 200)
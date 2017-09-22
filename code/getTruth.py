import urllib
import csv
from miner import *
import codecs
import exceptions
from string import rsplit, strip, split

from info_parser import TableParser
DEBUG = False


class rel_info_finder:
    def __init__(self, isubj, isubj_db_uri, iinc_path):
        self.subj = isubj
        self.mm = miner(DBPEDIA_URL_UP, isubj, isubj_db_uri)
        self.inc_path = iinc_path
        self.true_dict = {}
        self.fix_truth = {}
    
    def LOG(self, prow):
        log_file_name = "../results/" + self.subj + "/" + self.subj + "_getTruth_log.txt"
        if DEBUG:
            print prow
        else:
            with open(log_file_name, "a") as myfile:
                myfile.write(prow)
    
    def get_subj_from_uri(self,uri_strin):
        tsubj = rsplit(uri_strin,"/")[-1]
        return tsubj

    def get_related_objects_from_uri(self,row):
        
        try:
            list_of_related_objects = self.mm.update_so_dict(row[1], row[0])
        except (exceptions.Exception):
            self.LOG( "sparql error... ")
            return []
        names_ = [self.get_subj_from_uri(x) for x in list_of_related_objects]
        names = [n.replace('_', ' ') for n in names_ ]
        return names

    def example(self, subj, prop, nms):
        self.LOG( ["Subject: ", subj, "Prop: ", prop])
        self.true_dict[(subj, prop)] = None
        urls = "https://en.wikipedia.org/wiki/" + subj
        response = urllib.urlopen(urls).read()
        
        try:
            parser = TableParser(prop, nms)
            parser.feed(response)
            self.true_dict[(subj, prop)] = parser.res_dict
        except Exception as e:
            self.LOG( e)
            self.LOG( subj)

    def get_latest_from_true_dict(self,v_dict):
        """
        the function gets a dictionay with start and end time of the related objects:
        returns the one with the latest start time
        """
        max = ""
        max_obj = ""
        if v_dict == None:
            return ""
        for k,v in v_dict.items():
            if max == "" and len(v)>0:
                max = v[0]
                max_obj = k
            elif max != "" and len(v)>0 and max < v[0]:
                max = v[0]
                max_obj = k
        return max_obj, max



    def write_truth_to_csv(self, load=False):
        if load:
            rf_name = "../dumps/" + "/" + self.subj + "_truth_single_infobox.dump"
            if not os.path.exists(rf_name):
                return
            incs_file = open(rf_name, 'r')
            self.true_dict = pickle.load(incs_file)
            incs_file.close()

        for k,v in self.true_dict.items():
            try:
                cur_late, stm = self.get_latest_from_true_dict(v)
            except:
                self.LOG( "bad latest")
            self.fix_truth[k] = (cur_late, stm)

        csvf_name = "../results/" + self.subj + "/" + self.subj + "_truth_single_infobox.csv"
        with open(csvf_name, 'w') as csvfile:
            fieldnames = ['subject','property', 'current_or_latest', 'start_time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for (su,pu), (pt,stm) in  self.fix_truth.items():
                try:
                    uni_su = su.encode('utf-8')
                    uni_pu = pu.encode('utf-8')
                    cur = pt.encode('utf-8')
                    tm = stm.encode('utf-8')
                except:
                    uni_su = "encode-err"
                    uni_pu = "encode-err"
                    cur = "encode-err"
                    tm = "encode-err"

                writer.writerow({'subject': uni_su, 'property': uni_pu, 'current_or_latest': cur,'start_time': tm})


        csvfile.close()

    def auto_fix(self):
        self.LOG( 'auto_fix')
        violation_dict = {}
        
        with codecs.open(self.inc_path, mode='r', encoding=None, errors='replace', buffering=1) as csvfile:
            spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            i = 0
            for row in spamreader:

                row = ', '.join(row).split(',')
                self.LOG( row)
                if len(row) != 2 or ('\\' in row[0]) or i== 0:
                        i+=1
                        continue
                violation_dict[tuple(row)] = {}
                violation_dict[tuple(row)]['subjet_title'] = subj = self.get_subj_from_uri(row[0])
                # if DEBUG:
                #     if subj != "Anneliese_van_der_Pol":
                #         #continue
                violation_dict[tuple(row)]['property'] = prop = self.get_subj_from_uri(row[1])
                violation_dict[tuple(row)]['related_objects'] = objs = self.get_related_objects_from_uri(row)

                if len(objs) == 0:
                    sys.stdout.write("\b continued")
                    sys.stdout.write("\r")
                    sys.stdout.flush()
                    continue
                
                try:
                    self.example(subj, prop, objs)
                except:
                    self.LOG( "bad example" )
                sys.stdout.write("\b iter number: {}".format(i))
                sys.stdout.write("\r")
                sys.stdout.flush()
                i+=1
                if i > 40 and DEBUG:
                     break
        if DEBUG:
            for t in self.true_dict.items():
                self.LOG(t)
    
        dir_name = "../dumps"
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        dump_name = self.subj + "_truth_single_infobox.dump"
        inc_file = open(dir_name + "/" + dump_name, 'w')
        pickle.dump(self.true_dict, inc_file)
        inc_file.close()

        self.write_truth_to_csv(False)


if __name__ == '__main__':

    # '../results/BasketballPlayer_single_incs.csv'
    # 'person': "http://dbpedia.org/ontology/Person",
    fix_truth = {}
    rif = rel_info_finder('person',
                          "http://dbpedia.org/ontology/Person",
                          '../results/person/Person_temporal_csv.csv')
    #rif.write_truth_to_csv(True)

    rif.auto_fix()


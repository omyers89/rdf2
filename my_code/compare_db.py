from miner_base import *




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

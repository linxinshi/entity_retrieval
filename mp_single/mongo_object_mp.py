import sys
import string
import pymongo

class Mongo_Object(object):
      client = None
      db = None
      conn_dbs = None # include entitylist
      conn_wiki = None
      conn_e2v = None
      conn_q = None
      
      def __init__(self,hostname,port):
          self.client = pymongo.MongoClient(hostname,port)
          self.db = (self.client).test
          self.conn_dbs = self.db['dbpedia_sentence_new'] # include entitylist
          self.conn_wiki = self.db['wiki_fulltext']
          self.conn_e2v=self.db['entity2vec_final_smallRate']
          self.conn_q=self.db['query_entity']
      
      def __del__(self):
          (self.client).close()
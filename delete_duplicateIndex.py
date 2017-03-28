# mcl_clustering.mcl()
# average mentioned entity score  otherwise will be dominated by keyword matching case
import os, sys, argparse, datetime
import math, gensim, nltk
import numpy
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
from query_object_mpga import Query_Object
from entity_object_mpga import Entity_Object
from mongo_object_mpga import Mongo_Object
import string
import networkx
import copy

import lucene
from java.io import File
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, StringField, TextField, StoredField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader, Term
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.queryparser.classic import QueryParserBase, ParseException, QueryParser, MultiFieldQueryParser
from org.apache.lucene.search import IndexSearcher, Query, ScoreDoc, TopScoreDocCollector, TermQuery, TermRangeQuery
from org.apache.lucene.search.similarities import BM25Similarity

import pymongo

from Queue import PriorityQueue

# has java VM for Lucene been initialized
lucene_vm_init = False

# global data structure
queries = []
starttime=datetime.datetime.now()  

# global parameter
NA_ID = -1
hitsPerPage = 10000
LUCENE_INDEX_DIR='E:\\mmapDirectory\\index_mm_DB_py_more_abs_stem_bm25'
#QUERY_FILEPATH='E:\\Entity_Retrieval\\query\\query_debug_cluster.txt'
#queries_merged2_nodup_cluster_20170222.txt
QUERY_FILEPATH='E:\\Entity_Retrieval\\query\\merged2_cluster\\ordered_new3\\queries_merged2_nodup_cluster_20170223_fix3.txt'
REPORT_FILENAME='report.txt'
RESULT_FILENAME='pylucene.runs'

MAX_GRAPH_DEPTH=2
FROM_INDEX=0
FROM_DB=1

NEGATIVE_INFINITY=-9999999

cnt_batch=0
batch=[]

def cleanSentence(line):
    replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
    #line=str(line).translate(replace_punctuation).lower()      
    try:
       line = line.encode('utf-8').translate(replace_punctuation).lower()
    except Exception,e:
       print 'encode error'
       return line
    line=' '.join(line.split())
    return line.decode('utf-8')
    
def stemSentence(line,stemmer=SnowballStemmer('english')):
    line=cleanSentence(line)
    list=line.split(' ')
    stemlist=[stemmer.stem(word) for word in list]
    res=' '.join(stemlist)
    return res

def remove_duplicate(line):
    l=list(set(line.split(' ')))
    return ' '.join(l)

def read_query():
    src = open(QUERY_FILEPATH,'r')
    for line in src.readlines():
        list = line.strip().split('\t')
        queries.append((list[0],list[1],list[2],list[3])) # raw_ID,querystr(for w2v mark ngram),raw merge query, original query


def addDoc(w,title,name,value,category,skos_category,all_text,raw_name,raw_value,abstract):
    global batch,cnt_batch
    
    #print 'title='+title+ '  category='+category+'   skos='+skos_category
    doc = Document()
    doc.add(StringField('title',title,Field.Store.YES))
    doc.add(TextField('name',name,Field.Store.YES))
    doc.add(TextField('value',value,Field.Store.YES))
    doc.add(StoredField('category',category))
    doc.add(StoredField('skos_category',skos_category))
    doc.add(TextField('all_text',all_text,Field.Store.YES))
    doc.add(TextField('raw_name',raw_name,Field.Store.YES))
    doc.add(TextField('raw_value',raw_value,Field.Store.YES))
    doc.add(TextField('abstract',abstract,Field.Store.YES))
    
    #batch.append(doc)
    #cnt_batch+=1
    #if cnt_batch==1000:
    #   w.addDocuments(batch)
    #   cnt_batch=0
    #   del batch[:]
    w.addDocument(doc)
       
def main():
    global lucene_vm_init
    if not lucene_vm_init:
       lucene.initVM(vmargs=['-Djava.awt.headless=true'])
       lucene_vm_init = True
    
    is_index_Exist = os.path.exists(LUCENE_INDEX_DIR)
    # specify index path 
    index_mm = MMapDirectory(Paths.get(LUCENE_INDEX_DIR))
    
    # configure search engine
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    
    # load index to search engine
    reader = DirectoryReader.open(index_mm)
    searcher1 = IndexSearcher(reader)
    searcher1.setSimilarity(BM25Similarity())
    searcher2 = IndexSearcher(reader)
    w = IndexWriter(index_mm,config)
    # read query
    read_query()
    
    # initialize mongodb client
    mongoObj=Mongo_Object('localhost',27017)
      
    # search
    docDup=set()
    finalDup={}
    
    for i in xrange(len(queries)):
        print 'process query %d' %(i)
        query = queries[i]
        querystr = stemSentence(query[3])
        # build searcher
        q_lucene = QueryParser("all_text", analyzer).parse(querystr)
        collector = TopScoreDocCollector.create(hitsPerPage);
        searcher1.search(q_lucene, collector);
        hits = collector.topDocs().scoreDocs;
        
        
        # find candidate results after 1st round filter
        docDup.clear()
        for j in xrange(len(hits)):
            docID=hits[j].doc
            d=searcher1.doc(docID)
            if d['title'] in docDup:
               finalDup[d['title']]=d
               continue
            docDup.add(d['title'])
            
        docDup.clear()
        for j in xrange(len(hits)):
            docID=hits[j].doc
            d=searcher1.doc(docID)
            title=d['title']
            if d['title'] in docDup:
               continue
            docDup.add(title)
            
            item=(mongoObj.conn_me).find_one({'title':title})
            if item is None:
               continue
            entitylist=item['entitylist'].split('|')
            for en_title in entitylist:
                if title==en_title:
                   continue
                t=Term('title',en_title)
                q=TermQuery(t)
                docs=searcher2.search(q,2)
                if docs.totalHits<=1:
                   continue
                
                docID2=(docs.scoreDocs)[0].doc
                doc=searcher2.doc(docID2)
                finalDup[doc['title']]=doc

    print 'begin to clean index, there are %d dup records' %(len(finalDup))
    for title in finalDup:
        doc=finalDup[title]
        # title,name,value,category,skos_category,all_text,raw_name,raw_value,abstract
        
        name=doc['name']
        value=doc['value']
        category=doc['category']
        skos_category=doc['skos_category']
        all_text=doc['all_text']
        raw_name=doc['raw_name']
        raw_value=doc['raw_value']
        abstract=doc['abstract']
        
        print 'process '+title
        t=Term('title',title)
        q=TermQuery(t)
        w.deleteDocuments(q)
        addDoc(w,title,name,value,category,skos_category,all_text,raw_name,raw_value,abstract)
    # process remaining records
    #global batch,cnt_batch
    #if cnt_batch>0:
       #w.addDocuments(batch)
       #cnt_batch=0
       #del batch[:]
    w.close()
    
if __name__ == '__main__':
   reload(sys)
   sys.setdefaultencoding('utf-8')
   main()
   endtime = datetime.datetime.now()
   interval=(endtime - starttime).seconds
   print 'running time='+str(interval)
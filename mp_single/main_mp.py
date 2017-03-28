# mcl_clustering.mcl()

import os, sys, argparse
import math, numpy, gensim, nltk
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
from query_object_mp import Query_Object
from entity_object_mp import Entity_Object
from mongo_object_mp import Mongo_Object
import string

import lucene
from java.io import File
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, StringField, TextField, StoredField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.queryparser.classic import QueryParserBase, ParseException, QueryParser, MultiFieldQueryParser
from org.apache.lucene.search import IndexSearcher, Query, ScoreDoc, TopScoreDocCollector

import pymongo

from Queue import PriorityQueue

# has java VM for Lucene been initialized
lucene_vm_init = False

# global data structure
queries = []

# global parameter
hitsPerPage = 500
LUCENE_INDEX_DIR='E:\\mmapDirectory\\index_mm_DB_py_more_abs_stem'
#QUERY_FILEPATH='E:\\Entity_Retrieval\\query\\query_debug_cluster.txt'
QUERY_FILEPATH='E:\\Entity_Retrieval\\query\\merged2_cluster\\INEX_LD.txt'
lambda_text = 0.8
NEGATIVE_INFINITY=-9999999

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
    res=''
    for word in list:
        if word!=' ' and word!='':
           word=stemmer.stem(word)
           res=res+word+' '
    return res.strip()

def remove_duplicate(line):
    l=line.split(' ')
    l=list(set(l))
    res=''
    for word in l:
        if res=='':
           res=word
        else:
           res=res+' '+word
    return res.strip()
    
def read_query():
    src = open(QUERY_FILEPATH,'r')
    for line in src.readlines():
        list = line.strip().split('\t')
        queries.append((list[0],list[1])) # raw_ID,querystr
    
    print "print queries"
    for i in range(len(queries)):
        item = queries[i]
        print item[0]+" "+item[1]
    print 

def convStr2Vec(str):
    x=numpy.array(str.split('|'))
    return x.astype(numpy.float)
    
def computeScore(queryObj,entityObj,mongoObj,w2vmodel):

    global lambda_text  
    
    print 'compare document '+entityObj.title
    print 'queryID='+queryObj.queryID
    print 'entity text terms'
    print entityObj.enTerms
    
    # compute text_sim
    text_sim=0
    for i in range(len(queryObj.queryTerms)):
        maxSim=NEGATIVE_INFINITY
        maxTermPos=-1
        qtlist=queryObj.queryTerms[i]
        qtfinal=queryObj.qt_final[i]
        qtstem=queryObj.qt_stem[i]
        # find best alignment between each query cluster and entity term
        for j in range(len(entityObj.enTerms)):
            et=entityObj.enTerms[j]
            et_stem=entityObj.et_stem[j]
            # initialize local record for maxsim
            localMaxSim=NEGATIVE_INFINITY
            localTermPos=-1
            for k in range(len(qtlist)):
                qt=qtlist[k]
                qt_stem=qtstem[k]
                qt_final=qtfinal[k]
                # special case 1: totally equal , notice 
                if qt_stem==et_stem or qt==et_stem or qt_stem==et:
                   localMaxSim=1
                   localTermPos=k
                   break
                # inequal and not in w2vmodel , goto another sub query term
                if qt_final=='':
                   continue
                # check if all in w2vmodel , if not goto another sub query term though it is not in w2v but there may be perfect string matching 
                et_final=entityObj.et_final[j]
                if et_final=='':
                   continue
                # compute sim
                sim=w2vmodel.similarity(qt_final,et_final)
                if sim>localMaxSim:
                   localMaxSim=sim
                   localMaxTermPos=k
            # update maxim and maxtermpos from localMaxSim
            if localMaxSim>maxSim:
               maxSim=localMaxSim
               maxTermPos=j               
        # decide max sim and add to the total score                       
        if maxTermPos!=-1:
           text_sim+=maxSim
           #if maxSim>0:
           #   text_sim+=math.log(maxSim)
           #else:
           #   print 'domain error maxsim='+str(maxSim)
           print 'i='+str(i)+'  qt='+str(qtlist)+'  match et='+entityObj.enTerms[maxTermPos]+'   text maxSim='+str(maxSim)
    print 'text_sim='+str(text_sim)
    
    if len(queryObj.queryTerms)!=0:
       text_sim/=float(len(queryObj.queryTerms))
    print 'normalized text_sim='+str(text_sim)           
    # end computing text_sim
    
    print 'compute en sim'
    en_sim=0
    # number of qen that can be matched because some of them cannot be found in entity embedding
    cnt_qen_available=0
    
    for i in range(len(queryObj.q_ens)):
        q_en=queryObj.q_ens[i]
        maxSim=NEGATIVE_INFINITY
        maxEnPos=-1
        if queryObj.q_en_vector.has_key(q_en):
           qv=queryObj.q_en_vector[q_en]
        else:
           continue
        # find best alignment
        for j in range(len(entityObj.en_ens)):
            en_en=entityObj.en_ens[j]
            # special case totally equal
            if q_en==en_en:
               maxSim=1
               maxEnPos=j
               break
            if entityObj.en_vector.has_key(en_en):
               ev=entityObj.en_vector[en_en]
            else:
               continue      
            sim=abs(numpy.dot(qv,ev)/(numpy.linalg.norm(qv)*numpy.linalg.norm(ev)))  
            if sim>maxSim:
               maxSim=sim
               maxEnPos=j 
        # decide max sim and add to the total score 
        if maxEnPos!=-1:
           cnt_qen_available+=1
           en_sim+=maxSim
           #if maxSim>0:
           #   en_sim+=math.log(maxSim)
           #elif maxSim<0:
           #   en_sim+=math.log(-maxSim)
           print 'i='+str(i)+'  qen='+q_en+'  match en='+entityObj.en_ens[maxEnPos]+'   en maxSim='+str(maxSim)
    
    if cnt_qen_available!=0:
       en_sim/=float(cnt_qen_available)
    
    print 'cnt_qen_available='+str(cnt_qen_available)
    print 'en_sim='+str(en_sim)
    # consider wh-word similarity
        
    # compute and return overall score
    score = lambda_text*text_sim + (1-lambda_text)*en_sim
    print 'final score='+str(score)
    print 
    return score
    
def handle_process():
    quit()


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
    searcher = IndexSearcher(reader)
    
    # read query
    read_query()
    
    # initialize mongodb client
    mongoObj=Mongo_Object('localhost',27017)
      
    # initialize word2vec
    print 'load word2vec model'
    w2vmodel = gensim.models.Word2Vec.load_word2vec_format("F:\\modified_w2v\\w2v_wiki_trigram_phrase_20170101\\wiki.en.text.vector.binary", binary=True)
    print 'finish loading word2vec model'
    
    # search
    global hitsPerPage
    fields=['name','value']
    #parser=MultiFieldQueryParser(fields,analyzer)
    #parser.setDefaultOperator(QueryParserBase.AND_OPERATOR)
    rec_result=open('pylucene.runs','w')
    
    for i in range(len(queries)):
        query = queries[i]
        print 'processing query '+str(i)+':'+query[0]
        querystr = remove_duplicate(stemSentence(query[1]))
        #q_lucene=MultiFieldQueryParser.parse(parser,querystr)
        q_lucene = QueryParser("all_text", analyzer).parse(querystr)
        print "q_lucene: "+q_lucene.toString()
        collector = TopScoreDocCollector.create(hitsPerPage);
        searcher.search(q_lucene, collector);
        hits = collector.topDocs().scoreDocs;
        
        # build query object for computeScore
        queryObj=Query_Object(query,mongoObj,w2vmodel)
        
        # initialize duplicate remover
        docDup=set()
        
        # find candidate results after 1st round filter
        candidates = PriorityQueue()
        for j in range(len(hits)):
            docID=hits[j].doc
            d=searcher.doc(docID)
            name=cleanSentence(d['title'].strip())
            if name in docDup:
               continue
            docDup.add(name)
            # build entity object
            entityObj=Entity_Object(d,mongoObj,w2vmodel)
            score = computeScore(queryObj,entityObj,mongoObj,w2vmodel)
            #score=hits[j].score
            candidates.put((-score,j))
            
        # output results from priority queue larger score first
        rank=0
        while candidates.empty()==False and rank<100:
              rank=rank+1
              item=candidates.get()
              score=-item[0]
              j=item[1]  # index of hits[]
              docID=hits[j].doc
              d=searcher.doc(docID)
              title='<dbpedia:'+d.get('title')+'>'
              res_line=query[0]+'\t'+'Q0'+'\t'+title+'\t'+str(rank)+'\t'+str(score)+'\t'+'pylucene_multifield'
              rec_result.writelines(res_line+'\n')
    rec_result.close()
    
if __name__ == '__main__':
   reload(sys)
   sys.setdefaultencoding('utf-8')
   main()
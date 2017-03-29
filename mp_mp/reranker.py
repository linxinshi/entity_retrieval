from multiprocessing import Process,Manager
import os, sys, datetime, time, string
import math, gensim, numpy
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
from pattern.en import pluralize, singularize, lemma
from mongo_object_mpga import Mongo_Object
from entity_object_mpga import Entity_Object
from lib_process import cleanSentence, stemSentence, remove_duplicate, convStr2Vec, cleanDBpediaValue, categorySim
from Queue import Queue
import heapq
import random

PATH_WORD2VEC="F:\\modified_w2v\\w2v_wiki_trigram_phrase_20170101\\wiki.en.text.vector.binary"

class SmallObject(object):
    title=''
    list_category_term=None   # [ [t11,t12,t13], [t21,t22,t23..] ...   ]
    list_skos_category_term=None # [ [t11,t12,t13], [t21,t22,t23..] ...   ]
    raw_score=0.0
    raw_rank=-1
    
    def __init__(self,title):
        self.title=title

    def updateFromDB(self,title,mongoObj,w2vmodel):
        self.list_category_term=[]
        self.list_skos_category_term=[]
        item=(mongoObj.conn_acs).find_one({'title':self.title})
        if item is not None:
           list_category=item['raw_value'].strip('|').lower().split('|')
           for cat_str in list_category:
               list_term=self.updateTerms(cat_str,w2vmodel)
               if len(list_term)>0:
                  self.list_category_term.append(list_term)
          
        item=(mongoObj.conn_skos).find_one({'title':'Category:'+self.title})
        if item is not None:
           list_skos_category=item['raw_value'].strip('|').lower().replace(u'core#Concept|',u'').split('|')
           for skos_cat_str in list_skos_category:
               list_term=self.updateTerms(skos_cat_str,w2vmodel)
               if len(list_term)>0:
                  self.list_skos_category_term.append(list_term)

    def updateTerms(self,line,w2vmodel):
        list_term=line.split('_')
        list_result=[]
        
        whitelist=set(['win','won','most','biggest','largest','fastest'])
        blacklist=set(['give','also'])
        stoplist = set(stopwords.words('english'))

        for term in list_term:
            if term in blacklist:
               continue
            if term not in whitelist and term in stoplist:
               continue
            # find
            lem=lemma(term)
            sing=singularize(term)
            
            if term in w2vmodel.vocab:
               list_result.append(term)
            elif lem in w2vmodel.vocab:
                 list_result.append(lem)
            elif sing in w2vmodel.vocab:
                 list_result.append(sing)         
        return list_result
            

def rerank(scoreList_pair,mongoObj,w2vmodel):
    candidates=[]
    list_entity_pair=[]
    # scoreList_pair: entity,rank,score
    
    starttime=datetime.datetime.now()  
    for i in xrange(len(scoreList_pair)):
        title,score=scoreList_pair[i][0],scoreList_pair[i][2]
        obj=SmallObject(title)
        obj.updateFromDB(title,mongoObj,w2vmodel)
        list_entity_pair.append((obj,score))
        
    #endtime=datetime.datetime.now()
    #interval=(endtime - starttime).seconds      
    #print 'loading time='+str(interval)
    
    cnt_list_entity_pair=len(list_entity_pair)
    scoreList=[0.0 for i in xrange(cnt_list_entity_pair)]
    for i in xrange(cnt_list_entity_pair):
        en_score=0.0
        obj1=list_entity_pair[i][0]
        score1=list_entity_pair[i][1]
        for j in xrange(i+1,cnt_list_entity_pair):
            obj2=list_entity_pair[j][0]
            score2=list_entity_pair[j][1]
            cat_sim=categorySim(obj1,obj2,w2vmodel)
            temp_score=cat_sim*math.sqrt(score1*score2)
            scoreList[i]+=temp_score
            scoreList[j]+=temp_score
    for i in xrange(cnt_list_entity_pair):
        title=list_entity_pair[i][0].title
        heapq.heappush(candidates,(-scoreList[i]/float(cnt_list_entity_pair),title))
        
    list_result=[]
    rank=0
    while len(candidates)>0:
          rank+=1
          pair=heapq.heappop(candidates)
          list_result.append((pair[1],rank,pair[0]))
    return list_result
    
def handle_process(id_process,scoreList_pair,filename):
    starttime=datetime.datetime.now()  
 
    # initialize word2vec
    print 'id=%d  load word2vec model'%(id_process)
    w2vmodel = gensim.models.Word2Vec.load_word2vec_format(PATH_WORD2VEC, binary=True)
    print 'id=%d  finish loading word2vec model'%(id_process)
    
    mongoObj=Mongo_Object('localhost',27017)
    
    out_filename=filename.split('.')[0]+'_rerank_%d.runs'%(id_process)
    dest=open(out_filename,'w')
    
    cnt=0
    cnt_scorelist=len(scoreList_pair)
    for pair_score in scoreList_pair:
        # pair_score : (qid,scorelist[qid])
        # list_result (entity,rank,score)
        cnt+=1
        qid=pair_score[0]
        print 'id=%d   %d/%d  qid=%s'%(id_process,cnt,cnt_scorelist,qid)
        scoreList=pair_score[1]
        list_result=rerank(scoreList,mongoObj,w2vmodel)
        out_result=['%s\tQ0\t<dbpedia:%s>\t%d\t%f\tSGP\n'%(qid,pair[0],pair[1],-pair[2]) for pair in list_result]
        dest.writelines(out_result)
    dest.close()
    
    endtime=datetime.datetime.now()
    interval=(endtime - starttime).seconds
    print 'id=%d   running time=%s seconds'%(id_process,str(interval))    
    
def main(filename):
    scoreDict={}
    src=open(filename,'r')
    for line in src.readlines():
        l=line.strip().split('\t')
        qid=l[0]
        entity=l[2][9:-1]  # <dbpedia:entity>
        raw_rank=int(l[3])
        raw_score=float(l[4])
        if raw_score<0:
           continue
        if scoreDict.has_key(qid)==False:
           scoreDict[qid]=[]
        scoreDict[qid].append((entity,raw_rank,raw_score))
    src.close()
    
    scoreList_pair=[]
    for qid in scoreDict:
        scoreList_pair.append((qid,scoreDict[qid]))
    
    # begin multiprocessing
    cnt_list=len(scoreList_pair)
    process_list= []
    num_workers=5
    delta=cnt_list/num_workers  
    if cnt_list%num_workers!=0:  # +1 important
       delta=delta+1
      
    for i in xrange(num_workers):
        left=i*delta
        right=(i+1)*delta
        if right>cnt_list:
           right=cnt_list
         
        p = Process(target=handle_process, args=(i,scoreList_pair[left:right],filename))
        p.daemon = True
        process_list.append(p)

    for i in xrange(len(process_list)):
        process_list[i].start()
        print "sleep 50 seconds to enable next process"
        time.sleep(50)

    for i in xrange(len(process_list)):
        process_list[i].join()
    
    # begin merge result
    print 'begin merge result'
    list_result=[]
    for i in xrange(num_workers):
        out_filename=filename.split('.')[0]+'_rerank_%d.runs'%(i)
        src=open(out_filename,'r')
        list_result.extend(src.readlines())
        src.close()
        os.remove(out_filename)
        
    list_result.sort(key=lambda line:(line.split('\t')[0],int(line.split('\t')[3])),reverse=False)
    out_filename=filename.split('.')[0]+'_rerank.runs'
    dest=open(out_filename,'w')
    dest.writelines(list_result)
    dest.close()

if __name__ == '__main__':
   if len(sys.argv)<2:
      print 'too few arguments'
      print 'usage: python reranker.py runs_filename'
   elif len(sys.argv)>2:
      print 'too many arguments'
      print 'usage: python reranker.py runs_filename'
   else:
      if os.path.exists(sys.argv[1])==False:
         print 'error: file does not exist'
         quit()
      starttime=datetime.datetime.now() 
      main(sys.argv[1])
      endtime=datetime.datetime.now()
      interval=(endtime - starttime).seconds
      print 'total running time=%s seconds'%(str(interval))
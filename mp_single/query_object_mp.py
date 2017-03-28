import sys
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
import pymongo
import string
import numpy

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
    #line=cleanSentence(line)
    list=line.split(' ')
    res=''
    for word in list:
        if word!=' ' and word!='':
           word=stemmer.stem(word)
           res=res+word+' '
    return res.strip()

def remove_duplicate(line):
    ltmp=line.split(' ')
    l=list(set(ltmp))
    l.sort(key=ltmp.index)
    res=''
    for word in l:
        if res=='':
           res=word
        else:
           res=res+' '+word
    return res.strip()

def convStr2Vec(str):
    x=numpy.array(str.split('|'))
    return x.astype(numpy.float)    

class Query_Object(object):
      querystr=''
      queryID=''
      query_Other=''
      queryTerms=None
      qt_final=None
      qt_stem=None
      q_ens=None
      whwords_cur=set()
      q_en_vector=None
      
      def __init__(self,query,mongoObj,w2vmodel):
          self.queryID=query[0].strip()
          self.querystr=query[1].strip()
          self.update_ens(mongoObj)
          self.update_queryTerms(mongoObj,w2vmodel)
          self.update_q_en_vector(mongoObj)
          
      def update_queryTerms(self,mongoObj,w2vmodel):
          '''
          sub query str seperated by '|'
          now queryTerms and qt_final is a list of each sub query word list
          in other word,  [[],[],[]]
          '''
          whwords_ori=set(['what','where','when','who','which','whom','whose'])
          stemmer=SnowballStemmer('english')
          print 'querystr='+self.querystr
          subquery_tmp=self.querystr.split('|')
          
          self.queryTerms=[]
          self.qt_final=[]
          self.qt_stem=[]
          
          for i in range(len(subquery_tmp)):
              item=stemSentence(subquery_tmp[i])
              list_tmp=item.strip().split(' ')
              #print 'item='+item
              #print 'list_tmp='
              #print list_tmp
              list_res=[]
              for j in range(len(list_tmp)):
                  word=list_tmp[j]
                  if word in whwords_ori:
                     self.whwords_cur.add(word)
                  else:
                     list_res.append(word)
              (self.queryTerms).append(list_res)
          print 'query terms'
          print self.queryTerms
          # update word root
          for i in range(len(self.queryTerms)):
              list_tmp=self.queryTerms[i]
              qt_final_list_tmp=[]
              qt_stem_list_tmp=[]
              for j in range(len(list_tmp)):
                  qt=list_tmp[j]
                  qt_stem_term_tmp=stemmer.stem(qt)
                  # find root term in w2vmodel
                  qt_final_term_tmp=''
                  if qt in w2vmodel.vocab:
                     qt_final_term_tmp=qt
                  elif qt_stem_term_tmp in w2vmodel.vocab:
                     qt_final_term_tmp=qt_stem_term_tmp
                  qt_final_list_tmp.append(qt_final_term_tmp)
                  qt_stem_list_tmp.append(qt_stem_term_tmp)
              # finish this subquery and update list
              (self.qt_final).append(qt_final_list_tmp)
              (self.qt_stem).append(qt_stem_list_tmp)
              
      def update_ens(self,mongoObj):
          item = (mongoObj.conn_q).find_one({'ID':self.queryID})
          if item is not None:
             self.q_ens=item['entitylist'].split('|')
          if self.q_ens is None:
             self.q_ens=[]
          print 'q_ens'
          print self.q_ens
          
      def update_q_en_vector(self,mongoObj):
          self.q_en_vector={}
          for q_en in self.q_ens:
              ev_item=(mongoObj.conn_e2v).find_one({'title':q_en})
              if ev_item is None:
                 continue
              self.q_en_vector[q_en]=convStr2Vec(ev_item['vector'])
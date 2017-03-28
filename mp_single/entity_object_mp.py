'''

    need stopwords removal
    need mark bigrams and trigrams
'''
import sys
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
import pymongo
import string
import numpy

def remove_stopwords(line):
    list=line.split(' ')
    res=''
    whitelist=set(['win','won'])
    blacklist=set(['give','also'])
    stop = set(stopwords.words('english'))
    for word in list:
        if word in blacklist:
           continue
        elif word in whitelist:
           res=res+word+' '
        elif word not in stop:
           res=res+word+' '
    return res.strip()

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
    
def convStr2Vec(str):
    x=numpy.array(str.split('|'))
    return x.astype(numpy.float)
    
class Entity_Object(object):
      enstr=''
      title=''
      name=''
      raw_name=''
      value=''
      raw_value=''
      category=''
      skos_category=''
      abstract=''
      all_text=''
      enTerms=None
      et_final=None
      et_stem=None
      en_ens=None
      en_vector=None
      
      def __init__(self,entity,mongoObj,w2vmodel):
          # entity: dict   
          self.title=entity.get('title').strip()
          self.name=entity.get('name').strip()
          self.value=entity.get('value').strip()
          
          self.category=entity.get('category').strip()
          self.skos_category=entity.get('skos_category').strip()
          self.raw_name=entity.get('raw_name').strip()
          self.raw_value=entity.get('raw_value').strip()
          self.all_text=entity.get('all_text').strip()
          self.abstract=entity.get('abstract').strip()
          
          #self.enstr=self.name+' '+self.value
          #self.enstr=self.all_text
          self.enstr=self.raw_name+' '+self.raw_value+' '+self.category+' '+self.skos_category+' '+self.abstract
          
          self.update_ens(mongoObj)
          self.update_enTerms(mongoObj,w2vmodel)
          self.update_en_vector(mongoObj)
          
      def update_enTerms(self,mongoObj,w2vmodel):
          stemmer=SnowballStemmer('english')
          #self.enTerms=stemSentence(self.enstr,stemmer).split(' ')
          self.enTerms=remove_stopwords(cleanSentence(self.enstr)).split(' ')
          self.et_final=[]
          self.et_stem=[]
          
          # update word root
          for i in range(len(self.enTerms)):
              et=self.enTerms[i]
              et_stem=stemmer.stem(et)
              et_final_tmp=''
              (self.et_stem).append(et_stem)
              # find root term in w2vmodel
              if et in w2vmodel.vocab:
                 et_final_tmp=et
              elif et_stem in w2vmodel.vocab:
                   et_final_tmp=et_stem
              (self.et_final).append(et_final_tmp)
              
      def update_ens(self,mongoObj):
          self.en_ens=[]
          item=(mongoObj.conn_dbs).find_one({'title':self.title})
          if item is not None:
             self.en_ens=item['entitylist'].split('|')
          (self.en_ens).append(self.title)
          
      def update_en_vector(self,mongoObj):
          self.en_vector={}
          for en_en in self.en_ens:
              ev_item=(mongoObj.conn_e2v).find_one({'title':en_en})
              if ev_item is None:
                 continue
              self.en_vector[en_en]=convStr2Vec(ev_item['vector'])
          
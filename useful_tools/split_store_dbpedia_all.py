'''
based on dbpedia_new_sentence
include entities that do not have structured data
'''
import sys
import pymongo
import string
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import SnowballStemmer

# ===========================================================================

def cleanSentence(line):
    replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
    line=str(line).translate(replace_punctuation).lower()    
    line=' '.join(line.split())
    return line
    
def stemSentence(line,stemmer=SnowballStemmer('english')):
    line=cleanSentence(line)
    list=line.split(' ')
    stemlist=[stemmer.stem(word) for word in list]
    res=' '.join(stemlist)
    return res

def remove_duplicate(line):
    ltmp=line.split(' ')
    l=list(set(ltmp))
    l.sort(key=ltmp.index)
    res=' '.join(l)
    return res

def findName(line):
    res_temp = findRawName(line)
    res_str=''
    pos_head=0
    
    #print "now name="+res_temp	
    for pos_head in range(len(res_temp)-1):
        if res_temp[pos_head]=='(' or res_temp[pos_head]==')':
           continue
        elif res_temp[pos_head]=='_':
           res_str=res_str+" "
        elif res_temp[pos_head].islower() and res_temp[pos_head+1].isupper():
           res_str=res_str+res_temp[pos_head]+" "
        else:
           res_str=res_str+res_temp[pos_head]
    if len(res_temp)>0:
       if res_temp[-1] not in ['(',')','_']:
          res_str=res_str+res_temp[-1]
    
    return cleanSentence(res_str)
    
def findTitle(line):
    pos_head = line.find("resource/")
    pos_tail = -1
    return line[pos_head+9:pos_tail]
    
def findID(line):
    #print 'raw id ='+line
    pos_head=line.find('?oldid=')
    pos_tail=-1
    return line[pos_head+7:pos_tail]
    
def findRelation(line):
    return line.strip()[1:-1]
    
def findValue(line):
    # find quote , quote means value is a string identifier , otherwise it will be an uri
    flag_quote = line.find('\"') 
    if flag_quote > -1:
       pos_head = 1
       pos_tail = line.rfind('\"')
    else:
       pos_head = line.find("resource/")+9
       pos_tail = -1
    return line[pos_head:pos_tail]
# ===========================================================================

reload(sys)
sys.setdefaultencoding('utf-8')

# connect to the database
client = pymongo.MongoClient("localhost",27017)
db = client.test
table_connection=db['dbpedia3']
conn_dbs=db['dbpedia_new_sentence_more']
#filename ='debug.txt'


# read from dbpedia
src_dbpedia=open('mappingbased_properties_cleaned_en.nq','r')
#src_dbpedia=open('debug.txt','r')
rec_dbpedia={}

for line_src in src_dbpedia.readlines():
    if cmp(line_src,'# completed') == 0:
       break
    if len(line_src) == 0:
       break
    # first line
    if line_src[0] == '#':
       continue
    
    list_db = line_src.strip('.').strip().split('>')
    list_db[0]=list_db[0].strip()+'>'
    list_db[1]=list_db[1].strip()+'>'
    list_db[2]=list_db[2].strip()+'>'
    # list_db[2] has two situation
    title= findTitle(list_db[0])
    relation = findRelation(list_db[1])
    value = findValue(list_db[2])
    pair=relation+r'%%%%'+value
    #print 'title=%s  relation=%s  value=%s '%(title,relation,value)
    #print 'title=%s  pair=%s'%(title,pair)
    if rec_dbpedia.has_key(title)==False:
       rec_dbpedia[title]=[]
    rec_dbpedia[title].append(pair)
    
src_dbpedia.close()

# iterate from title list
cnt_batch=0
batch=[]
filename = 'titles_labels_en.nq'
#filename = 'debug_title.txt'
src_title = open(filename,'r')
duplicate_record=set()

for line_src in src_title.readlines():
    line_src = line_src.strip()
    if cmp(line_src,'# completed') == 0:
       break
    if len(line_src) == 0:
       break
    # first line
    if line_src[0] == '#':
       continue

    list_title = line_src.strip('.').strip().split('>')
    list_title[0]=list_title[0].strip()+'>'
    list_title[1]=list_title[1].strip()+'>'
    list_title[2]=list_title[2].strip()+'>'
       

    title= findTitle(list_title[0])
    label = findRelation(list_title[1])
    
    list_tmp=list_title[2].split('<')
    list_tmp[0]=list_tmp[0].strip()
    list_tmp[1]='<'+list_tmp[1].strip()
    value_title = findValue(list_tmp[0])
    id=findID(list_tmp[1])
    
    #print 'title=%s  label=%s  value=%s  id=%s'%(title,label,value_title,id)
    if title in duplicate_record:
       continue
    duplicate_record.add(title)

    value=''
    relation=''
    if rec_dbpedia.has_key(title):
       list_rel_pair=rec_dbpedia[title]
       value='$$$$'.join(list_rel_pair)
       list_rel=[term.split('%%%%')[0] for term in list_rel_pair]
       relation='|'.join(list_rel)
    
    # find record in dbpedia_new_sentence
    item_dbs=conn_dbs.find_one({'title':title})
    name=''
    abstract=''
    entity=''
    # id title value relation    name abstract entity 
    if item_dbs is not None:
       name=item_dbs['name']
       abstract=item_dbs['abstract']
       entity=item_dbs['entity']
       
    # store into database
    item={'id':id,'title':title,'value':value,'relation':relation,'name':name,'abstract':abstract,'entity':entity}
    #item =  {'title':title,'name':name,'value':value,'abstract':abstract,'entity':entity,'relation':relation,'id':id}
    batch.append(item)
    cnt_batch+=1
    if cnt_batch==6000:
       item_id = table_connection.insert_many(batch)
       cnt_batch=0
       batch=[]
    #print 'title='+entity_title+' '+'   name='+entity_name+'  value='+sentence+'   entity:'+entity_value

# process remaining record
if cnt_batch>0:
   item_id = table_connection.insert_many(batch)
   cnt_batch=0
   batch=[]
   
client.close()    
src_title.close()


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

def findRawName(line):
    flag_url = line.find("http")
    if flag_url > -1:
       pos_head = line.rfind('/')+1
       pos_tail = -1
    else:
       pos_head = 0
       pos_tail = -1
       
    return line[pos_head:pos_tail]

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

def findValue(line):
    flag_quote = line.find('\"')  # find quote
    if flag_quote > -1:
       pos_head = 1
       pos_tail = line.rfind('\"')
    else:
       pos_head = line.rfind('/')+1
       pos_tail = -1       

    res_temp = line[pos_head:pos_tail]
    
    if flag_quote > -1:
       res_str = makeSentence(res_temp)
    else:
       res_str = res_temp
       
    return cleanSentence(res_str)
# ===========================================================================

reload(sys)
sys.setdefaultencoding('utf-8')

# connect to the database
client = pymongo.MongoClient("localhost",27017)
db = client.test
table_connection=db['dbpedia_new_sentence_more']
conn_abstract = db['long_abstracts']
conn_dbs=db['dbpedia_new_sentence']
#filename ='debug.txt'
filename = 'titles_labels_en.nt'
src = open(filename,'r')

cnt_batch=0
batch=[]

for line_src in src.readlines():
    line_src = line_src.strip()
    if cmp(line_src,'# completed') == 0:
       break
    if len(line_src) == 0:
       break
    # first line
    if line_src[0] == '#':
       continue
   
    list = line_src.split('>')
    title= findRawName(list[0].strip()+'>')
    name = cleanSentence(findName(list[0].strip()+'>'))
    
    # find record in long_abstracts
    item_abs=conn_abstract.find_one({'title':title})
    if item_abs is not None:
       abstract=item_abs['value'].strip()
    else:
       abstract=''
       
    # find record in dbpedia_new_sentence
    item_dbs=conn_dbs.find_one({'title':title})
    if item_dbs is not None:
       entity=item_dbs['entity'].strip('|')
       value=item_dbs['value']
    else:
       entity='NONE'
       value=''
       
    # store into database
    item =  {'title':title,'name':name,'value':value,'abstract':abstract,'entity':entity}
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
src.close()


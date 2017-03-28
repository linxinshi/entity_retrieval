import sys
import pymongo
import string

# ===========================================================================
def cleanSentence(line):
    replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
    line=str(line).translate(replace_punctuation).lower()    
    line=' '.join(line.split())
    return line

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

# ===========================================================================
reload(sys)
sys.setdefaultencoding('utf-8')

# connect to the database
client = pymongo.MongoClient("localhost",27017)
db = client.test
conn_title=db['title2id']
conn_wiki=db['wiki_fulltext']

bulk_wiki = conn_wiki.initialize_ordered_bulk_op()
cnt_batch=0
for item in conn_title.find():
    cnt_batch+=1
    title=item['title']
    raw_name=item['raw_name']
    
    #conn_wiki.update({'name':raw_name},{},upsert=False)
    bulk_wiki.find({'name':raw_name}).update({'$set':{'title':title}})
    if cnt_batch==1000:
       bulk_wiki.execute()
       bulk_wiki = conn_wiki.initialize_ordered_bulk_op()
# process remaining record
if cnt_batch>0:
   bulk_wiki.execute()
   bulk_wiki = conn_wiki.initialize_ordered_bulk_op()
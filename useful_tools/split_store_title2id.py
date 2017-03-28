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

def findTitle(line):
    pos_head = line.find("resource/")
    pos_tail = -1
       
    return line[pos_head+9:pos_tail]

def findName(line):
    #print 'raw name = '+line
    pos_head=line.find('\"')
    pos_tail=line.rfind('\"')
    return line[pos_head+1:pos_tail]

def findID(line):
    #print 'raw id ='+line
    pos_head=line.find('?oldid=')
    pos_tail=-1
    return line[pos_head+7:pos_tail]
# ===========================================================================
reload(sys)
sys.setdefaultencoding('utf-8')

# connect to the database
client = pymongo.MongoClient("localhost",27017)
db = client.test
table_connection=db['title2id']
#filename ='debug.txt'
filename = 'titles_labels_en.nq'
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
    line_tmp = list[2].strip()+'>'
    list_tmp = line_tmp.split('<')
    
    title= findTitle(list[0].strip()+'>')
    raw_name = findName(list_tmp[0].strip())
    id = findID('<'+list_tmp[1].strip())
    
    item={'title':title,'raw_name':raw_name,'name':cleanSentence(title),'id':id}
    batch.append(item)
    cnt_batch+=1
    if cnt_batch==7000:
       item_id = table_connection.insert_many(batch)
       cnt_batch=0
       batch=[]    
      
    #print title+'\t'+raw_name+'\t'+id

# process remaining record
if cnt_batch>0:
   item_id = table_connection.insert_many(batch)
   cnt_batch=0
   batch=[]
   
src.close()
client.close()
import sys
import string

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

def main():
   # parse arguments
   if len(sys.argv)<3:
      print 'too few arguments'
      print 'FORMAT: erra Submitted_Results GroundTruth'
      quit()
   elif len(sys.argv)>3:
      print 'too many arguments'
      print 'FORMAT: erra Submitted_Results GroundTruth'
      quit()
   else:
      path_result=sys.argv[1]
      path_truth=sys.argv[2]
   
   # read data
   src_result=open(path_result,'r')
   src_truth=open(path_truth,'r')
   if src_result is None:
      print 'unavailable access to submitted result'
      src_result.close()
      src_truth.close()
      quit()
   if src_truth is None:
      print 'unavailable access to groundtruth'
      src_result.close()
      src_truth.close()
      quit()
   
   # process data
   groundtruth=set()
   groundtruth_cleaned=set()  # cleaned version of answer 
   for line in src_truth.readlines():
       list_tmp=line.strip().split('\t')
       id=list_tmp[0].strip()
       ans=list_tmp[2].strip()
       groundtruth.add((id,ans))
       groundtruth_cleaned.add((id,cleanSentence(ans)))
   src_truth.close()
   
   # check submitted results
   incorrectSpell=[]
   for line in src_result.readlines():
       list_tmp=line.strip().split('\t')
       id=list_tmp[0].strip()
       ans=list_tmp[2].strip()
       if (id,ans) in groundtruth:
          groundtruth.remove((id,ans))
       elif (id,cleanSentence(ans)) in groundtruth_cleaned:
          incorrectSpell.append( (id,cleanSentence(ans)) )
   src_result.close()
   
   print 'MISSING GROUNDTRUTH'
   l=list(groundtruth)
   l=sorted(l,key=lambda pair:pair[0])
   for i in range(len(l)):
       pair=l[i]
       print pair[0]+'\t'+pair[1]
       if i>0 and l[i-1][0]!=l[i][0]:
          print
       
   print '\n'
   print 'POSSIBLE INCORRECT SPELLING ANSWER'
   incorrectSpell=sorted(incorrectSpell,key=lambda pair:pair[0])
   for item in incorrectSpell:
       print item[0]+'\t'+item[1]
   

if __name__ == '__main__':
   reload(sys)
   sys.setdefaultencoding('utf-8')
   main()
import argparse as ap
import json

import snappy
import porterStemmer as ps
import re
import timeit
parser =  ap.ArgumentParser()
parser.add_argument("queryfile")
parser.add_argument("resultfile")
parser.add_argument("indexfile")
parser.add_argument("dictfile")

args = parser.parse_args()

begin = timeit.default_timer()

ps1 = ps.PorterStemmer()
def decodeC1(byteSet):
    postings = []
    i = 0
    temp  = ''
    while(i < len(byteSet)):
        thisByte = byteSet[i:i+1]
        binary = "{:08b}".format(int(thisByte.hex(),16))
        #print(binary)
        if(binary[0] == '1' ):
            temp +=  binary[1:]
        elif(binary[0] == '0'):
            temp +=  binary[1:]
            #print(temp)
            temp = '0b' + temp
            #print(temp)
            postings.append(int(temp,2))
            temp = ''
        
        i += 1

    return postings
def intersection(list1,list2):
    # i = 0
    # j = 0
    # ans = []
    # while( i < len(list1) and j < len(list2)):
    #     if(list1[i] == list2[j]):
    #         ans.append(list1[i])
    #         i += 1
    #         j += 1
    #     elif(list1[i] < list2[j]):
    #         i += 1
    #     else:
    #         j += 1
    
    return set(list1).intersection(list2)

def gapDecoder(myList):
    answer = []
    temp = 0
    for i in range(len(myList)):
        answer.append(myList[i] + temp)
        temp = answer[i]
    return answer


queryFile = open(args.queryfile,'r')
resultFile = open(args.resultfile,'w+')
#indexFile = open("indexfile.idx",'rb')
dictFile = open(args.dictfile,'r')

dictFileData = json.load(dictFile)
myDict = dictFileData["terms"]
docidmap = dictFileData["docids"]
compressedBY = dictFileData["compression"]
qINdex = 0
for line in queryFile.readlines():
    queries = re.split(r"[-;:\"\',.\s()!]\s*",line.strip())
    #print(queries)
    for i in range(len(queries)):
        queries[i] = ps1.stem(queries[i].lower(),0,len(queries[i])-1)
    
    #print(myDict[query])
    if(len(queries) == 1):
        query = queries[0].strip()
        termPostings = []

        if query in myDict:
            
            
            postingIndexArr = myDict[query]
            start = postingIndexArr[0]
            end = postingIndexArr[1]
            if(compressedBY == 0):
                numTerms = int((end - start)/4)

                with open(args.indexfile,'rb') as indexFile:
                    skip = indexFile.read(start)
                    for i in range(int(numTerms)):
                        byte = indexFile.read(4)
                        termPostings.append(int.from_bytes(byte,'big'))
            elif(compressedBY == 3):
                seriesOfBytes = b''
                with open(args.indexfile,'rb') as indexFile:
                    skip = indexFile.read(start)
                    seriesOfBytes = indexFile.read(end -start)
                seriesOfBytes = snappy.decompress(seriesOfBytes)
                for i in range(0,len(seriesOfBytes),4):
                    byte = seriesOfBytes[i:i+4]
                    termPostings.append(int.from_bytes(byte,'big'))
                
                termPostings = gapDecoder(termPostings)
            elif(compressedBY == 1):
                seriesOfBytes = b''
                with open(args.indexfile,'rb') as indexFile:
                    skip = indexFile.read(start)
                    seriesOfBytes = indexFile.read(end -start)
                termPostings = decodeC1(seriesOfBytes)
                termPostings = gapDecoder(termPostings)
                


            
        
        
        if (len(termPostings) > 0 ):
            for posting in termPostings:
                resultFile.write("Q" + str(qINdex))
                resultFile.write(" ")
                resultFile.write(str(docidmap[str(posting)]))
                resultFile.write(" ")
                resultFile.write("1")
                resultFile.write('\n')
        
    elif(len(queries) > 1):
        listSet = {}
        for query in queries:
            termPostings = []

            if query in myDict:
                #print(query)
                
                postingIndexArr = myDict[query]
                start = postingIndexArr[0]
                end = postingIndexArr[1]
                if(compressedBY == 0):
                    numTerms = int((end - start)/4)

                    with open(args.indexfile,'rb') as indexFile:
                        skip = indexFile.read(start)
                        for i in range(int(numTerms)):
                            byte = indexFile.read(4)
                            termPostings.append(int.from_bytes(byte,'big'))
                elif(compressedBY == 3):
                    seriesOfBytes = b''
                    with open(args.indexfile,'rb') as indexFile:
                        skip = indexFile.read(start)
                        seriesOfBytes = indexFile.read(end -start)
                    seriesOfBytes = snappy.decompress(seriesOfBytes)
                    for i in range(0,len(seriesOfBytes),4):
                        byte = seriesOfBytes[i:i+4]
                        termPostings.append(int.from_bytes(byte,'big'))
                
                    termPostings = gapDecoder(termPostings)
                elif(compressedBY == 1):
                    seriesOfBytes = b''
                    with open(args.indexfile,'rb') as indexFile:
                        skip = indexFile.read(start)
                        seriesOfBytes = indexFile.read(end -start)
                    termPostings = decodeC1(seriesOfBytes)
                    termPostings = gapDecoder(termPostings)
                
                

            
            listSet[query] = termPostings
            

        intersectionList = listSet[queries[0]].copy()
        for i in range(1,len(queries)):
            temp = intersection(intersectionList,listSet[queries[i]])
            intersectionList = temp.copy()


        if (len(intersectionList) > 0 ):
            for posting in intersectionList:
                resultFile.write("Q" + str(qINdex))
                resultFile.write(" ")
                resultFile.write(str(docidmap[str(posting)]))
                resultFile.write(" ")
                resultFile.write("1")
                resultFile.write('\n')         




    
        

    qINdex += 1
    
terminate = timeit.default_timer()

print(terminate - begin)





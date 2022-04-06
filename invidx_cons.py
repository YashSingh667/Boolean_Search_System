
from bs4 import BeautifulSoup as bs
import lxml

import re
import porterStemmer as ps
import argparse as ap
import os
import json

from os import listdir
from os.path import isfile, join
import snappy

import timeit

parser = ap.ArgumentParser()


parser.add_argument("collpath")
parser.add_argument("indexfile")
parser.add_argument("stopwordfile")
parser.add_argument("compression")
parser.add_argument("xmltags")

args = parser.parse_args()
#file_list = os.listdir('tipster-ap-frac/')
begin = timeit.default_timer()
def encodeC1(myInt):
    binary = bin(myInt)[2:]
    while(len(binary) % 7 != 0):
        binary = '0' + binary
    byteSet = b''
    for i in  range(0,len(binary),7):
        temp = binary[i:i+7]
        if(i == len(binary) - 7):
            temp =  '0b0' + temp
            
        else:
            temp = '0b1' + temp
        #print('encoded temp  ' + temp)
        tempInt = int(temp,2)
        #print('encoded int ' + str(tempInt))

        byteSet +=  tempInt.to_bytes(1,'big')
    
    return byteSet


def gapEncoder(myList):
    answer= []
    
    temp = 0
    for i in range(len(myList)):
        answer.append(myList[i] - temp) 
        temp = myList[i]
    return answer

def gapDecoder(myList):
    answer = []
    temp = 0
    for i in range(len(myList)):
        answer.append(myList[i] + temp)
        temp = answer[i]
    return answer


file_list = files_path = [os.path.join(args.collpath,x) for x in os.listdir(args.collpath)]

#print(len(file_list))
#print(file_list)
dictionary = {}

docidsToInt = {}
docIndex = 1

numTempFiles = 1
compressBy = int(args.compression)

if(compressBy == 2 or compressBy == 4 or compressBy == 5):
    print('not implemented')
    exit()
stopWordFile  = args.stopwordfile
with open(stopWordFile) as file:
    stopwords= file.readlines()
    stopwords = [line.rstrip() for line in stopwords]

xmlTagFile = args.xmltags
with open(xmlTagFile) as file :
    tagsToParse = file.readlines()
    tagsToParse = [line.rstrip() for line in tagsToParse]
ps1 = ps.PorterStemmer()

for i in range(len(file_list)):
    with open(file_list[i], 'r') as f:        
        content = f.readlines()
        content = "".join(content)
        soup = bs(content, "lxml")
        
        
        #soup.prettify()
        docs = soup.find_all('doc')    
        docids = soup.find_all('docno')

        dictForFile = {}

        for docid in docids:
            docidsToInt[docid.text] = docIndex
            docIndex += 1   

        #print(docidsToInt)


        #textArr = []
        locfilePostingsList = {}

        #docTexts = soup.find_all('text')

        for i in range(len(docs)):
            #textSet = docs[i].find_all('text')

            text = ""
            for j in range(1,len(tagsToParse)):
                tagset = docs[i].find_all(tagsToParse[j].lower())
                for textCurr in tagset:
                    text += " " + textCurr.text


            
            currDocID = docidsToInt[docs[i].find('docno').text]
            #textArr.append(text)
            #print(text)
            tempTokens = re.split(r"[-;:\"\',.\s()!]\s*",text)

            localDict = []
            
            for k in range(len(tempTokens)):
                if tempTokens[k].lower() in stopwords:
                    continue
                else:
                    currTerm = ps1.stem(tempTokens[k].lower(),0,len(tempTokens[k])-1)
                    
                    if currTerm in localDict:
                        continue
                    else:
                        localDict.append(currTerm)
            
                  
            
            for x in sorted(localDict):
                if x in locfilePostingsList:                    
                    locfilePostingsList[x].append(currDocID)
                else:                    
                    locfilePostingsList[x] = [currDocID]

            
            
        

        #print(len(textArr))

        filePath = str(numTempFiles)

        numTempFiles += 1

        locFile = open(filePath,"wb+")

        fileByteIndex = 0

        for i in sorted(locfilePostingsList):
            #locFile.write(i)
            #locFile.write('---')
            
            dictForFile[i] = [fileByteIndex]
            for x in range(len(locfilePostingsList[i])):
                currInt = locfilePostingsList[i][x]
                

                locFile.write(currInt.to_bytes(4,'big'))
                
                fileByteIndex += 4
                #locFile.write('  ')
            
            dictForFile[i].append(fileByteIndex)
        locFile.close()
        for term in sorted(dictForFile):
            if term in dictionary:
                dictionary[term][filePath] = dictForFile[term]
            else:
                dictionary[term] = {filePath : dictForFile[term] }

        
        
        

        
        

        
        #print(len(dictionary))


    

#mergeAllPostings
finalIndexFileName = args.indexfile + '.idx'
finalDict = {}
finalIndex = open(finalIndexFileName,'wb+')
iterFinalIndex = 0
for term in sorted(dictionary):
    termFiles = dictionary[term]
    termPostings = []
    for file in termFiles:
        start = dictionary[term][file][0]
        end = dictionary[term][file][1]
        numTerms = (end - start)/4
        checkerArr = []

        with open(file,'rb') as currFile:
            skip = currFile.read(start)
            for i in range(int(numTerms)):
                byte = currFile.read(4)
                termPostings.append(int.from_bytes(byte,'big'))
                

           
            
        
    if(compressBy == 0):    
        finalDict[term] = [iterFinalIndex]
        for posting in termPostings:
            finalIndex.write(posting.to_bytes(4,'big'))
            iterFinalIndex += 4
        
        finalDict[term].append(iterFinalIndex)
    elif(compressBy == 3):
        finalDict[term] = [iterFinalIndex]
        seriesOfBytes = b''
        termPostings = gapEncoder(termPostings)
        for posting in termPostings:
            seriesOfBytes += posting.to_bytes(4,'big')
        seriesOfBytes = snappy.compress(seriesOfBytes)
        iterFinalIndex += len(seriesOfBytes)
        finalIndex.write(seriesOfBytes)
        finalDict[term].append(iterFinalIndex)
    elif(compressBy == 1):
        finalDict[term] = [iterFinalIndex]
        myByteSeries = b''
        termPostings = gapEncoder(termPostings)
        for posting in termPostings:
            myByteSeries += encodeC1(posting)
        finalIndex.write(myByteSeries)
        iterFinalIndex +=  len(myByteSeries)
        finalDict[term].append(iterFinalIndex)

        


finalIndex.close()
for i in range(len(file_list)):
    os.remove(str(i+1))

dictFileDict = {}
dictFileDict["compression"] = compressBy
dictFileDict["terms"] = finalDict
dictFileDict["docids"] = dict((v, k) for k, v in docidsToInt.items())
finalDictFileName = args.indexfile + '.dict'
with open(finalDictFileName,'w+') as dictFile:
    json.dump(dictFileDict,dictFile)

terminate = timeit.default_timer()

print(terminate - begin)






        


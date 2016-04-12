
import sys,json

def postprocess(inputFileName,dictFileName,outputFileName,ignoreLength):
    with open(inputFileName,'r') as inputFile,open(dictFileName,'r') as dictFile,open(outputFileName,'w') as outFile:
        dictionary = json.load(dictFile)
        for line in inputFile:
            line = line.strip()
            nodes = line.split(' ')
            if len(nodes) <=ignoreLength:
                continue
            for node in nodes:
                if node in dictionary:
                    outFile.write(dictionary[node] + "\t")
            outFile.write("\n")




if __name__ == '__main__':
    inputFileName = sys.argv[1]
    reverseDictFile = sys.argv[2]
    outputFileName = sys.argv[3]
    ignoreLength = int(sys.argv[4])
    postprocess(inputFileName,reverseDictFile,outputFileName,ignoreLength)
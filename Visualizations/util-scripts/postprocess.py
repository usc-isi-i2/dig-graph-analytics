import sys
import json
import operator

def helper(inputFileName,dictFileName,node_type,outputFileName):
    cc_dict = {}
    with open(inputFileName,'r') as inputFile,open(dictFileName,'r') as dictFile:
        dictionary = json.load(dictFile)
        for line in inputFile:
            line = line.strip()
            graph_node = line.split('\t')[0]
            cc_node_num = line.split('\t')[1]
            if cc_node_num in cc_dict:
                cc_dict[cc_node_num].append(dictionary[graph_node])
            else:
                cc_dict[cc_node_num] = [dictionary[graph_node]]


    with open(outputFileName,'w') as outputFile:
        for tuple in sorted(cc_dict.items(),key=lambda x:len(x[1]),reverse=True):
            for node in tuple[1]:
                if node_type == '-1':
                    outputFile.write(node + '\t')
                if node_type in node:
                    outputFile.write(node + "\t")
            outputFile.write('\n')

if __name__ == '__main__':
    inputFileName = sys.argv[1]
    dictFileName = sys.argv[2]
    node_type = sys.argv[3]
    outputFileName = sys.argv[4]
    helper(inputFileName,dictFileName,node_type,outputFileName)
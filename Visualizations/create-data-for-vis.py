
import sys
import json


# seller : 1, email : 3, phone : 2

def helper(inputFileName,dictFileName,outputFileName,clique_length):
    jsonObj = {"nodes":[],"links":[]}
    vis_dictionary = {}
    dictionary = json.load(open(dictFileName))
    with open(inputFileName,'r') as inputFile:
        counter = 0
        for line in inputFile:
            line = line.strip()
            vertices = line.split(' ')
            if len(vertices) <= clique_length:
                continue
            for vertex in vertices:
                if vertex not in vis_dictionary:
                    vis_dictionary[vertex] = counter
                    counter = counter+1
                    value = dictionary[vertex]
                    if 'seller' in value:
                        jsonObj['nodes'].append({"name":value,"group":1})
                    elif 'phone' in value:
                        jsonObj['nodes'].append({"name":value,"group":2})
                    else:
                        jsonObj['nodes'].append({"name":value,"group":3})


    with open(inputFileName,'r') as inputFile:
        for line in inputFile:
            line = line.strip()
            vertices = line.split(' ')
            if len(vertices) <=clique_length :
                continue
            for i in range(len(vertices)):
                for j in range(i+1,len(vertices)):
                    jsonObj['links'].append({ "source":  int(vis_dictionary[vertices[i]]),  "target":  int(vis_dictionary[vertices[j]]),  "value":  1 })

    with open(outputFileName,'w') as outputFile:
        outputFile.write(json.dumps(jsonObj,indent=4))






if __name__ == '__main__':
    inputFileName = sys.argv[1]
    dictFileName = sys.argv[2]
    outputFileName = sys.argv[3]
    clique_length = int(sys.argv[4])
    helper(inputFileName,dictFileName,outputFileName,clique_length)


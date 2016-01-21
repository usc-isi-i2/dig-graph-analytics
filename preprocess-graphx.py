from digSparkUtil.fileUtil import FileUtil
from pyspark import SparkContext
import json
import argparse


def f(x,y):
    print y

def preprocess(sc,inputDir,file_format,outputDir):
    """
    this method just reads the offer file and creates vertexrdd and edgerdd required for graphx
    vertexrdd will be node uri and type
    edgesrdd will be node a,node b,edge type
    :param inputDir:
    :param file_format:
    :return:
    """
    fileUtil = FileUtil(sc)
    inputrdd=fileUtil.load_file(inputDir,file_format=file_format,data_type='json')
    vertexrdd = inputrdd.flatMapValues(lambda x:nodes_mapper(x))
    #rdd = vertexrdd.foreach(lambda (x,y):f(x,y))

    edgerdd = inputrdd.flatMapValues(lambda x : edges_mapper(x))
    fileUtil.save_file(vertexrdd,outputDir+'vertex',file_format='text',data_type='json')
    fileUtil.save_file(edgerdd,outputDir+'edges',file_format='text',data_type='json')


def nodes_mapper(line):
    if isinstance(line,str):
        jsonObj = json.loads(line)
    else:
        jsonObj = line
    if jsonObj['a'] == 'Offer':
        if 'uri' in jsonObj:
            yield jsonObj['uri'],jsonObj['a']
        if 'availableAtOrFrom' in jsonObj and jsonObj['availableAtOrFrom']['a']=='Place':
            yield jsonObj['availableAtOrFrom']['uri'],jsonObj['availableAtOrFrom']['a']
        if 'itemOffered' in jsonObj:
            yield jsonObj['itemOffered'],'AdultService'
        if 'mainEntityOfPage' in jsonObj:
            yield jsonObj['mainEntityOfPage'],'WebPage'
        if 'seller' in jsonObj:
            yield jsonObj['seller'],'Seller'



def edges_mapper(line):
    if isinstance(line,str):
        jsonObj = json.loads(line)
    else:
        jsonObj = line
    if jsonObj['a'] == 'Offer':
        offerUri = jsonObj['uri']
        if 'availableAtOrFrom' in jsonObj and jsonObj['availableAtOrFrom']['a']=='Place':
            yield offerUri,jsonObj['availableAtOrFrom']['uri'],'availableAtOrFrom'
        if 'itemOffered' in jsonObj:
            yield offerUri,jsonObj['itemOffered'],'itemOffered'
        if 'mainEntityOfPage' in jsonObj:
            yield offerUri,jsonObj['mainEntityOfPage'],'mainEntityOfPage'
        if 'seller' in jsonObj:
            yield offerUri,jsonObj['seller'],'seller'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--inputFile",help = "input File path",required=True)
    parser.add_argument("--file_format",help = "file format text/sequence",default='sequence')
    parser.add_argument("-o","--output_dir",help = "output File path",required=True)


    args = parser.parse_args()
    sc = SparkContext()
    preprocess(sc,args.inputFile,args.file_format,args.output_dir)















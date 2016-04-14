from pyspark import SparkContext
from digSparkUtil.fileUtil import FileUtil
import sys
import json
import math


def helper(tuple):
    if isinstance(tuple[1],str):
        jsonObj = json.loads(tuple[1])
    else:
        jsonObj = tuple[1]
    seller_uri = jsonObj['uri']
    #print seller_uri,type(seller_uri),hash(str(seller_uri))
    phone_uris = list()
    email_uris = list()
    if 'telephone' in jsonObj:
        telephones = jsonObj['telephone']
        phone_uris = list()
        for telephone in telephones:
            phone_uris.append(telephone['uri'])
    if 'email' in jsonObj:
        emails = jsonObj['email']
        email_uris = list()
        for email in emails:
            email_uris.append(email['uri'])

    line = {}
    if len(phone_uris) >0 :
        line ["phones"] = phone_uris
    if len(email_uris) > 0:
        line ["emails"] = email_uris

    yield (str(seller_uri),line)

    for phone in phone_uris:
        temp = list(phone_uris)
        temp.remove(phone)
        line = {"seller_uri" : seller_uri}
        if len(temp) >0:
            line["phones"] = temp
        if len(email_uris) > 0:
            line ["emails"] = email_uris
        yield (str(phone),line)

    for email in email_uris:
        temp = list(email_uris)
        temp.remove(email)
        line = {}
        if len(phone_uris) > 0:
            line["phones"] = phone_uris
        if len(temp) > 0:
            line["emails"] = temp
        yield (str(email),line)


def hash(uri):
    hash = 7
    for c in uri:
        hash = hash * 31 + ord(c)
    return str(abs(hash % 982451653))

def f1(x):
    print type(x[1])


def wrapper(tuple):
    key = tuple[0]
    for object in list(tuple[1]):
        if 'seller_uri' in object:
            yield key + "\t" + object['seller_uri']
        if 'phones' in object:
            for phone in object['phones']:
                yield key + "\t" + phone
        if 'emails' in object:
            for email in object['emails']:
                yield key + "\t" + email



if __name__ == '__main__':
    inputFileName = sys.argv[1]
    file_type = sys.argv[2]
    outputFileName = sys.argv[3]

    sc = SparkContext(appName="Generate Data for Max Cliques")
    fileUtil = FileUtil(sc)
    input_rdd = fileUtil.load_file(inputFileName,file_format=file_type)
    # input_rdd = input_rdd.sample(False, 0.5, seed=1234)
    # input_rdd.foreach(lambda x : f1(x))
    intermediate_rdd = input_rdd.flatMap(lambda x : helper(x))
    #fileUtil.save_file(intermediate_rdd,"intermediate_1",file_format="text")
    intermediate_rdd = intermediate_rdd.groupByKey()

    #output_rdd.foreach(lambda x : f(x))
    intermediate_rdd = intermediate_rdd.flatMap(lambda x : wrapper(x))

    intermediate_rdd = intermediate_rdd.coalesce(1)
    intermediate_rdd.saveAsTextFile(outputFileName)
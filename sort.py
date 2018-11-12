# encoding: utf-8
from pyspark import SparkContext, SparkConf
import json
# var conf = new SparkConf()
conf = SparkConf().setAppName("My app").setMaster("local")
sc = SparkContext(conf=conf)
# textFile = sc.textFile("file:///home/xiang/Desktop/sogou.500w.utf8")
# textFile = sc.textFile("/sparksorts")
textFile = sc.textFile("/sogou_ext/20111230")
uIDRdd = textFile.filter(lambda x:x.split("\t")[1]=="02a8557754445a9b1b22a37b40d6db38")
usearch=uIDRdd.map(lambda x:(x.split("\t")[2],1)).reduceByKey(lambda x,y:x+y)
usearch=usearch.sortBy(lambda x:x[1],False)
lists = usearch.take(15)
for i in lists:
    # print json.dumps(i,encoding='utf-8',ensure_ascii=False)
    print(i)

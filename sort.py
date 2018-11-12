# encoding: utf-8
from pyspark import SparkContext, SparkConf
from collections import defaultdict


# var conf = new SparkConf()
conf = SparkConf().setAppName("My app").setMaster("local")
sc = SparkContext(conf=conf)


# textFile = sc.textFile("file:///home/xiang/Desktop/sogou.500w.utf8")
# textFile = sc.textFile("/sparksorts")
textFile = sc.textFile("/sogou_ext/20111230")
#每人搜索次数(id,time)
pairs = textFile.map(lambda x: (
    x.split("\t")[1], 1)).reduceByKey(lambda x, y: x+y)
#time
lens=pairs.values()
#每种time的个数
res=sorted(lens.countByValue().items())
#返回的是dict格式
for i in res:
    print (i)
# uIDRdd=uIDRdd.sortBy(lambda x:x[1],ascending=False)

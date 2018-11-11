# encoding: utf-8
from pyspark import SparkContext, SparkConf

# var conf = new SparkConf()
conf = SparkConf().setAppName("My app").setMaster("local")
sc = SparkContext(conf=conf)
# textFile = sc.textFile("file:///home/xiang/Desktop/sogou.500w.utf8")
# textFile = sc.textFile("/sparksorts")
textFile = sc.textFile("/sogou_ext/20111230")
uIDRdd = textFile.map(lambda x: (
    x.split("\t")[1], 1)).reduceByKey(lambda x, y: x+y)
uIDRdd=uIDRdd.sortBy(lambda x:x[1],ascending=False)
lists = uIDRdd.take(10)
for i in lists:
    print(i)

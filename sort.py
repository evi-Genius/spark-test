# encoding: utf-8
from pyspark import SparkContext, SparkConf

# var conf = new SparkConf()
conf = SparkConf().setAppName("My app").setMaster("local")
sc = SparkContext(conf = conf)
# textFile = sc.textFile("file:///home/xiang/Desktop/sogou.500w.utf8")
textFile = sc.textFile("/sparksorts")
textFile.persist()
print(textFile.count())
lists=textFile.collect()#collct把RDD转成list
for i in lists[0:30]:
    print(i)
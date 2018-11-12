# encoding: utf-8
from pyspark import SparkContext, SparkConf

def show(a):
    time=a[1]//33
    str=""
    while time:
        time-=1
        str=str+"*"
    return (a[0],str)
# var conf = new SparkConf()
conf = SparkConf().setAppName("My app").setMaster("local")
sc = SparkContext(conf=conf)
# textFile = sc.textFile("file:///home/xiang/Desktop/sogou.500w.utf8")
# textFile = sc.textFile("/sparksorts")
textFile = sc.textFile("/sogou_ext/20111230")
fs=textFile.filter(lambda x:x.split("\t")[2]==u"人体艺术")
pairs=fs.map(lambda x:(x.split("\t")[6]+x.split("\t")[7]+x.split("\t")[8]+x.split("\t")[9],1))
res=pairs.reduceByKey(lambda x,y:x+y)
res=res.sortByKey(ascending=True)
res.persist()
show=res.map(show)
lis=res.collect()
for i in lis:
    print(i)
lis=show.collect()
for i in lis:
    print(i)
# uIDRdd=uIDRdd.sortBy(lambda x:x[1],ascending=False)


from pyspark.mllib.fpm import FPGrowth
from pyspark.sql import SQLContext
from pyspark.context import SparkConf
from pyspark import SparkContext
import time

starttime=time.time()
conf = SparkConf().setAppName("graphframe").set("spark.executor.memory", "2g")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
min=0.02
data=sc.textFile("/spark_test/new.txt").cache()
transactions= data.map(lambda line: line.strip().split(' '))
model=FPGrowth.train(transactions,minSupport=min,numPartitions=10)
result=sorted(model.freqItemsets().collect())
f = open ('/home/spark_test/resultsweb.txt','w')
count=0
for fi in result:
    if(len(fi.items)!=1):
        f.write(str(fi.items)+","+str(fi.freq))
        f.write('\n')
        count+=1

f.write("Total: %d" % count)

f.close()
endtime=time.time()

print("Minsupoort is %d in %d transactions" % (int(transactions.count()*min),transactions.count()))
print("Generate %d set in %.3f seconds" % (count,endtime-starttime))
from pyspark.mllib.fpm import FPGrowth
from pyspark.sql import SQLContext
from pyspark.context import SparkConf
from pyspark import SparkContext
import time

starttime=time.time()
conf = SparkConf().setAppName("graphframe").set("spark.executor.memory", "2g")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

data=sc.textFile("/spark_test/new.txt").cache()
transactions= data.map(lambda line: line.strip().split(' '))
model=FPGrowth.train(transactions,minSupport=0.05,numPartitions=10)
result=sorted(model.freqItemsets().collect())
print(len(result))
f = open ('/home/spark_test/resultsweb.txt','w')
print(result[0])
for fi in result:
    f.write(str(fi))
    f.write('\n')
f.close()
endtime=time.time()
print("花费时间：", endtime-starttime)
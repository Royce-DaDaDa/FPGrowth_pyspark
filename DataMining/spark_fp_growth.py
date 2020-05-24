from pyspark import SparkConf, SparkContext,AccumulatorParam
import time
#from DataMining.fp_growth import FPNode,FP_Growth
class FPNode:
    def __init__(self,id,count,parent):
        self.id=id
        self.count=count
        self.parent=parent
        self.children=[]
    def add(self):
        self.count+=1
    def show(self,indent):
        print(" "*indent+self.id+" "+str(self.count))
        indent+=2
        for node in self.children:
            node.show(indent)
    def mergenodes(self,minsup):
        ids=[i.id for i in self.children]
        if len(ids) == 0:
            return
        idset = set(ids)
        iddict = {i:[] for i in idset}
        for index,i in enumerate(ids):
            iddict[i].append(index)
        newchildren=[]
        for (k,v) in iddict.items():
            nnode=FPNode(k,0,self)
            for n in v[:]:
                nnode.count+=self.children[n].count
                nnode.children+=self.children[n].children
            newchildren.append(nnode)
        del self.children
        self.children=newchildren
        for node in self.children:
            node.mergenodes(minsup)


class FP_Growth:
    def __init__(self,minsup,dataset,freqset):
        self.minsup=minsup
        self.freqset=freqset
        self.dataset=dataset
        self.FPTree=FPNode("Root",0,None)
        self.patterns=[]
        self.linktable={i[0]: [] for i in sorted(self.freqset.items(), key=lambda x: x[1])}
    def createfreqset(self):
        rawset={}
        for row in self.dataset:
            for i in row:
                if i in rawset.keys():
                    rawset[i]+=1
                else:
                    rawset[i]=1

        self.freqset={i:j for i,j in rawset.items() if j>=self.minsup}
        self.linktable={i[0]:[] for i in sorted(self.freqset.items(),key=lambda x:x[1])}

    def getfreq(self,elem):
        return self.freqset[elem]

    def createfptree(self):
        for row in self.dataset:
            row=[i for i in row if i in self.freqset.keys()]
            row.sort(key=self.getfreq,reverse=True)
            pnode=self.FPTree
            for item in row:
                flag=False
                for n in pnode.children:
                    if n.id==item:
                        n.count+=1
                        pnode=n
                        flag=True
                        break
                if not flag:
                    node=FPNode(item,1,pnode)
                    pnode.children.append(node)
                    pnode=node
                    self.linktable[pnode.id].append(pnode)
        #self.FPTree.show(0)
        #self.FPTree.show(0)

    def createcontree(self,id,prefix,linktable):
        contree=FPNode("Root",0,None)
        for node in linktable[id]:
            pnode=node
            nnode=FPNode(pnode.parent.id,pnode.count,None)
            pnode=pnode.parent
            if pnode.id=="Root":
                continue
            while pnode.parent.id != "Root" :
                tnode=nnode
                nnode=FPNode(pnode.parent.id,tnode.count,None)
                tnode.parent=nnode
                nnode.children.append(tnode)
                pnode=pnode.parent
            nnode.parent=contree
            contree.children.append(nnode)
        contree.mergenodes(self.minsup)
        contable={}
        freqdict={}
        self.createcontable(contree,contable,freqdict)
        contable=dict(sorted(contable.items(),key=lambda x:self.freqset[x[0]]))
        for (k,v) in freqdict.items():
            if v>=self.minsup:
                self.patterns.append((tuple(prefix+[k]),v))
        for i in contable.keys():
            self.createcontree(i,prefix+[i],contable)
        #contree.show(0)
        #self.findpattern([id],contree)

    def createcontable(self,contree,contable,freqdict):
        if contree.id!="Root":
            #if contree.count>=self.minsup:
                if contree.id in freqdict.keys():
                    freqdict[contree.id]+=contree.count
                else:
                    freqdict[contree.id]=contree.count
                if contree.id in contable.keys():
                    contable[contree.id].append(contree)
                else:
                    contable[contree.id]=[contree]
                for i in contree.children:
                    self.createcontable(i,contable,freqdict)
            # else:
            #     contree.delete()
        else:
            for i in contree.children:
                self.createcontable(i, contable,freqdict)

    def buildpatterns(self):
        for i in self.linktable.keys():
            self.createcontree(i,[i],self.linktable)


starttime=time.time()
conf = SparkConf().setAppName("spark_fp_growth2")
sc = SparkContext(conf=conf)

path="/spark_test/new.txt"
textfile=sc.textFile(path)

length=textfile.count()
minsup=int(length*0.05)

def filtermin(item):
    if item[1] >= minsup:
        return True
    else:
        return False


freqset = textfile.flatMap(lambda line: line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a, b : a + b).filter(filtermin).sortBy(lambda x:x[1],False)

broadfreq=sc.broadcast(freqset.collectAsMap()).value

def buildfpgrowth(t):
    fp = FP_Growth(0, t, broadfreq)
    fp.createfptree()
    fp.buildpatterns()
    return fp.patterns

rawpattern = textfile.flatMap(lambda line: [line.split(" ")]).repartition(4).mapPartitions(buildfpgrowth).reduceByKey(lambda a, b : a + b).filter(filtermin).collect()

f = open('/home/spark_test/patterns.txt','w')
count=0
for i in rawpattern:
    f.write(str(i))
    f.write("\n")
    count+=1
print("Generate %d set" % count)
f.close()
sc.stop()
endtime=time.time()
print(endtime-starttime)
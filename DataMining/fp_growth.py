import time
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
    def delete(self):
        self.parent.children.remove(self)
        for i in self.children:
            del i
        del self


class FP_Growth:
    def __init__(self,minsup,dataset):
        self.minsup=minsup
        self.linktable={}
        self.freqset={}
        self.dataset=dataset
        self.FPTree=None
        self.patterns=[]
        self.itemset=[]
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
        self.itemset=list(self.linktable.keys())
    def getfreq(self,elem):
        return self.freqset[elem]

    def createfptree(self):
        self.FPTree=FPNode("Root",0,None)
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
                self.patterns.append([prefix+[k],v])
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

a=time.time()
dataset=[]
count=0
total=0
with open("D:/new.txt","r",encoding="utf-8") as f:
    while True:
        line=f.readline()
        if not line:
            break
        c=line.split(" ")
        if "文字版" in c and "的" in c: #and "的" in c and "新":
             count+=1
        dataset.append(c)
        total+=1
# dataset=[
#                 ['I1','I2','I5'],
#                 ['I2','I4'],
#                 ['I2','I3'],
#                 ['I1','I2','I4'],
#                 ['I1','I3'],
#                 ['I2','I3'],
#                 ['I1','I3'],
#                 ['I1','I2','I3','I5'],
#                 ['I1','I2','I3']
#               ]

print(count)
minsup=int(total*0.05)
#minsup=2
print(minsup)
fp=FP_Growth(minsup,dataset)
fp.createfreqset()
fp.createfptree()
#fp.createcontree("文字版")
fp.buildpatterns()
b=time.time()
print(b-a)
print(len(fp.patterns))
import json
import jieba
import os
import re
filepath=r"D:\news_broadcast_corpus-master\news_content\\"
filelist=os.listdir(filepath)
print(len(filelist))
output=r"D:/new.txt"
count=0
with open(output,"w",encoding="utf-8") as out:
    for i in filelist:
        with open(os.path.join(filepath,i),encoding="utf-8") as f:
            load_json=json.load(f)
            text=load_json["text"]
            sentences=re.split(r'[.!?;:。！？：；]',text)
            for sent in sentences:
                r ='[’!"#$%&\'()*+,-./:;<=>?@^_`[\\]{|}~\n。！，“”；：+·。、《》（）0-9【】a-z]+'
                line = re.sub(r, '', sent)
                if len(line)!=0:
                    words = list(set(jieba.cut(line)))
                    if " " in words:
                        words.remove(" ")
                    out.write(" ".join(words)+"\n")
                count+=1

out.close()
print(count)

# with open(output,"r",encoding="utf-8") as input:
#     a=input.readline()
#     a=input.readline()
#     print(a)

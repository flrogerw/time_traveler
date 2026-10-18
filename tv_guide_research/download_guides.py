import json,re,urllib.parse,subprocess,os,concurrent.futures as cf
jobs=[]
# meta.json / meta2.json = `curl -sL https://archive.org/metadata/<item>` for vintage-tv-guides and
# tv-guide-collection_202108 (the two archive.org items the TV Guide scans come from)
d1=json.load(open("meta.json"))
for f in d1["files"]:
    n=f["name"]
    if n.endswith("_djvu.xml") and n.startswith("TV Guide"):
        jobs.append(("vintage-tv-guides",n,"guides/xml1/"+n[9:19]+".xml"))
d2=json.load(open("meta2.json"))
names={f["name"] for f in d2["files"]}
for n in sorted(names):
    if n.endswith("_djvu.xml") and re.search(r"19(7|8)\d",n) and not n.endswith("_text_djvu.xml"):
        jobs.append(("tv-guide-collection_202108",n,"guides/xml2/"+re.sub(r"[^A-Za-z0-9._-]+","_",n)))
print(len(jobs),"files to fetch",flush=True)
def get(j):
    item,n,out=j
    if os.path.exists(out) and os.path.getsize(out)>10000: return (out,1)
    subprocess.run(["curl","-sL","--retry","3","-o",out,f"https://archive.org/download/{item}/"+urllib.parse.quote(n)])
    return (out,os.path.exists(out) and os.path.getsize(out)>10000)
ok=0
with cf.ThreadPoolExecutor(6) as ex:
    for i,(out,good) in enumerate(ex.map(get,jobs)):
        ok+=bool(good)
        if i%25==0: print(i,ok,flush=True)
print("done",ok,"of",len(jobs),flush=True)

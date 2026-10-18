import json,re,collections
recs=[json.loads(l) for l in open("listings2_1965_70.jsonl")]
def norm_tag(t):
    t=re.sub(r"[^a-z ]","",(t or "").lower()).strip()
    return t
CLASSES={
 "cartoons/children":("cartoon","children","juvenile"),
 "western":("western",),
 "comedy":("comedy","sitcom"),
 "adventure/sci-fi":("adventure","science","fantasy"),
 "drama":("drama",),
 "crime/mystery/suspense":("crime","mystery","suspense","detective","horror","spy"),
 "variety/music":("variety","music"),
 "game/quiz":("game","quiz","panel"),
 "serial (soap)":("serial",),
 "religion":("religio",),
 "education":("education","lessons","science","instruction"),
}
def cls(tag):
    for c,keys in CLASSES.items():
        if any(tag.startswith(k) or (" "+k) in (" "+tag) for k in keys): return c
def slot(day,t):
    if t is None: return None
    h=t/60
    if day=="saturday": return "SAT 07-12" if 7<=h<12 else "SAT 12-19" if 12<=h<19 else None
    if day=="sunday": return "SUN 07-12" if 7<=h<12 else "SUN 12-19" if 12<=h<19 else None
    return "WKD 06-10" if 6<=h<10 else "WKD 10-16" if 10<=h<16 else "WKD 16-19" if 16<=h<19 else None
cols=["SAT 07-12","SAT 12-19","SUN 07-12","SUN 12-19","WKD 06-10","WKD 10-16","WKD 16-19"]
tab=collections.defaultdict(collections.Counter)
for r in recs:
    c=cls(norm_tag(r["tag"])); s=slot(r["day"],r["time"])
    if c and s: tab[c][s]+=1
print("Where each kind of show (by the GUIDE's own genre tag) actually aired -- % of that kind's listings, 1965-70, all stations")
print(f"{'':26}"+"".join(f"{c:>11}" for c in cols)+"    n")
for c in CLASSES:
    n=sum(tab[c].values())
    if n: print(f"{c:26}"+"".join(f"{100*tab[c][k]/n:>10.0f}%" for k in cols)+f"  {n}")
# how much of each slot is each kind
print("\nShare of each time slot by kind (of tagged listings):")
print(f"{'':26}"+"".join(f"{c:>11}" for c in cols))
tot={k:sum(tab[c][k] for c in tab) for k in cols}
for c in CLASSES:
    print(f"{c:26}"+"".join(f"{100*tab[c][k]/tot[k]:>10.0f}%" for k in cols))
# explicit check: crime/mystery/suspense tagged titles in Saturday 07-12
print("\nSaturday 07-12 listings tagged crime/mystery/suspense/horror/spy:")
c=collections.Counter()
for r in recs:
    if r["day"]=="saturday" and r["time"] is not None and 7*60<=r["time"]<12*60 and cls(norm_tag(r["tag"]))=="crime/mystery/suspense":
        c[(r["title"][:30],r["tag"][:18])]+=1
print(sum(c.values()),"of",tab["crime/mystery/suspense"]["SAT 07-12"]+0, c.most_common(12))

import sys, glob, json, os, re, time, multiprocessing as mp
sys.path.insert(0, ".")
from parse_guide import parse_issue

def year_of(path):
    base = os.path.basename(path)
    m = re.search(r"(19[6-9]\d)", base)
    return int(m.group(1)) if m else None

def work(path):
    issue = os.path.basename(path)[:-4]
    try:
        recs = parse_issue(path, issue)
        y = year_of(path) if "xml2" in path else int(issue[:4])
        for r in recs: r["year"] = y
        return issue, recs, None
    except Exception as e:
        return issue, [], repr(e)

if __name__ == "__main__":
    files = sorted(glob.glob("guides/xml1/*.xml")) + sorted(glob.glob("guides/xml2/*.xml"))
    # drop exact duplicate issues in collection 2 (same listing counts under two file names)
    out = open("listings_all.jsonl", "w")
    stats = []
    with mp.Pool(6) as pool:
        for issue, recs, err in pool.imap_unordered(work, files):
            days = {r["day"] for r in recs}
            for r in recs: out.write(json.dumps(r) + "\n")
            stats.append((issue, len(recs), len(days), err))
    out.close()
    stats.sort()
    print(len(stats), "files;", sum(s[1] for s in stats), "listings")
    bad = [s for s in stats if s[3] or s[1] < 700 or s[2] < 7]
    print("problem files:", bad)

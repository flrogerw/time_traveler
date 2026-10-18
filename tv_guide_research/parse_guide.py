"""
Parse a TV Guide djvu.xml (OCR with word coordinates) into structured listings -- v2.

Start times are assigned by PAGE COORDINATES (the nearest time marker above a title in the same page
half), because in some layouts (e.g. summer 1969) the OCR emits the time column separately from the
titles. Layout facts (checked against page images, 1965-01 and 1969-07): two columns per page; a big
weekday word at the top of a day's first page; bold time markers at the left margin of a column, one
per time block; each program is channel icons, then a bold CAPS title with an optional "-Tag"
("-Cartoons", "-Children", "-Western"...), an optional "(C)" color mark, then a description whose
end carries "(NN min.)" for long programs.

Record: issue, day, page, half, time (minutes after midnight; None before the day's first marker),
title, tag, icons, minutes, desc.
"""
import difflib
import re
import xml.etree.ElementTree as ET

DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
TIME_RE = re.compile(r"^(\d{1,2})[:.](\d{2})$")
DUR_RE = re.compile(r"\((?:(\d+)\s*min\.?|(one|two|three|four)\s+hours?|(\d+)\s*hrs?\.?)[^)]*\)", re.I)
WORDNUM = {"one": 60, "two": 120, "three": 180, "four": 240}
SUBHEADS = {"morning", "afternoon", "evening", "late evening", "morning-afternoon", "afternoon-evening",
            "late show", "night", "late night", "late"}
COLOR_RE = re.compile(r"\(\s*[CcOo©]\s*\)|\(\s*Cc\s*\)")
JUNK_TITLES = {"TV GUIDE", "CLOSE-UP", "A TV GUIDE", "TV CLOSE-UP GUIDE", "CLOSE UP", "SPECIAL", "COLOR", "LIVE"}


def day_of(word_text):
    t = re.sub(r"[^a-z]", "", word_text.lower())
    if len(t) < 5:
        return None
    best = max(DAYS, key=lambda d: difflib.SequenceMatcher(None, t, d).ratio())
    return best if difflib.SequenceMatcher(None, t, best).ratio() >= 0.8 else None


def upper_ratio(text):
    letters = [ch for ch in text if ch.isalpha()]
    return sum(ch.isupper() for ch in letters) / len(letters) if len(letters) >= 3 else 0.0


def line_words(line):
    out = []
    for w in line.findall("WORD"):
        c = [int(v) for v in w.get("coords").split(",")]
        out.append({"t": w.text or "", "l": c[0], "b": c[1], "r": c[2], "top": c[3], "h": c[1] - c[3],
                    "conf": int(w.get("x-confidence") or 0)})
    return out


GLUED_RE = re.compile(r"^(\d{1,2})(?:[:.](\d{2}))?([AaPp])\.?[Mm]\.?$")
AMPM_RE = re.compile(r"^([AaPp])\.?[Mm]\.?$")


def time_at(ws, k):
    """A time marker starting at ws[k]: '9:30', '10AM', '9 AM', '9:30 PM' -> (tokens, hh, mm, ampm)."""
    t = ws[k]["t"]
    m = GLUED_RE.match(t)
    if m and 1 <= int(m.group(1)) <= 12:
        return 1, int(m.group(1)), int(m.group(2) or 0), m.group(3).lower() + "m"
    m = TIME_RE.match(t)
    if m and 1 <= int(m.group(1)) <= 12 and int(m.group(2)) < 60:
        if k + 1 < len(ws) and AMPM_RE.match(ws[k + 1]["t"]):
            return 2, int(m.group(1)), int(m.group(2)), AMPM_RE.match(ws[k + 1]["t"]).group(1).lower() + "m"
        return 1, int(m.group(1)), int(m.group(2)), None
    if t.isdigit() and 1 <= int(t) <= 12 and k + 1 < len(ws) and AMPM_RE.match(ws[k + 1]["t"]):
        return 2, int(t), 0, AMPM_RE.match(ws[k + 1]["t"]).group(1).lower() + "m"
    return None


def split_rows(ws, scale=1.0):
    """One OCR LINE can hold words from two visual rows; group words by vertical position."""
    ws = sorted(ws, key=lambda w: (w["top"], w["l"]))
    rows, cur = [], [ws[0]]
    for w in ws[1:]:
        ref = cur[0]
        if abs(w["top"] - ref["top"]) <= 0.55 * max(ref["h"], 20 * scale):
            cur.append(w)
        else:
            rows.append(sorted(cur, key=lambda x: x["l"]))
            cur = [w]
    rows.append(sorted(cur, key=lambda x: x["l"]))
    return rows


def parse_issue(path, issue):
    root = ET.parse(path).getroot()
    records = []
    day = None
    last_abs = None  # last resolved absolute minute within the day
    prev_last_time = None
    for page_no, page in enumerate(root.iter("OBJECT")):
        width = int(page.get("width") or 1534)
        height = int(page.get("height") or 2200)
        scale = width / 1517.0   # scans range from ~780 to ~3200 px wide; size cutoffs scale with it
        lines = []
        for col in page.iter("PAGECOLUMN"):
            for line in col.iter("LINE"):
                ws = line_words(line)
                if ws:
                    lines.extend(split_rows(ws, scale))
        if not lines:
            continue

        # day header: a large weekday word in the top part of the page
        for ws in lines:
            for w in ws:
                if w["top"] < 0.16 * height and w["h"] >= 34 * scale:
                    d = day_of(w["t"])
                    if d and d != day:
                        day, last_abs, prev_last_time = d, None, None
        if day is None:
            continue

        # time markers: time-looking tokens near the left margin of their half
        # columns: the right column's rows begin at its own left margin, so put the split just left of the
        # smallest row-start x in the right 60% of the page (time markers can sit left of centre)
        starts = [ws[0]["l"] for ws in lines if ws[0]["h"] < 90 * scale]
        right_start = min([x for x in starts if x >= 0.40 * width] or [width // 2])
        x_split = right_start - 15
        half_of = lambda x: 0 if x < x_split else 1
        margin = {0: min([w["l"] for ws in lines for w in ws if half_of(w["l"]) == 0] or [0]),
                  1: min([w["l"] for ws in lines for w in ws if half_of(w["l"]) == 1] or [width // 2])}
        text_margin = {
            hh: min([w["l"] for ws in lines for w in ws
                     if half_of(w["l"]) == hh and not TIME_RE.match(w["t"])] or [margin[hh]])
            for hh in (0, 1)
        }
        markers = []  # (half, top, hh, mm, ampm)
        for ws in lines:
            for k in range(len(ws)):
                ti = time_at(ws, k)
                if ti:
                    h = half_of(ws[k]["l"])
                    if ws[k]["l"] - margin[h] <= 0.075 * width + (0.05 * width if h == 1 else 0):
                        markers.append((h, ws[k]["top"], ti[1], ti[2], ti[3]))
        markers.sort(key=lambda m: (m[0], m[1]))
        resolved = []
        for h, top, hh, mm, ampm in markers:
            base = (hh % 12) * 60 + mm
            ref = last_abs if last_abs is not None else 5 * 60
            if ampm:
                a = base + (720 if ampm == "pm" else 0)
                if a < ref - 60 and a < 6 * 60:
                    a += 1440          # 12AM-5AM after the day's evening
            else:
                a = min((c for c in (base, base + 720, base + 1440) if c >= ref - 25), default=base + 1440)
            last_abs = a
            resolved.append((h, top, a))
        prev_page_last = prev_last_time
        left_last = max((a for mh, _, a in resolved if mh == 0), default=None)

        def time_for(h, top):
            best = None
            for mh, mtop, a in resolved:
                if mh == h and mtop <= top + 10:
                    best = a
            if best is None:
                # above this half's first marker: continue from the left column (right half) or from
                # the previous page's last marker (left half)
                best = left_last if h == 1 and left_last is not None else prev_page_last
            return best

        pending = None
        recs_page = []
        for ws in lines:
            text = " ".join(w["t"] for w in ws)
            low = re.sub(r"[^a-z\- ]", "", text.lower()).strip()
            if low in SUBHEADS:
                continue
            drop = set()
            for k in range(min(3, len(ws))):
                ti = time_at(ws, k)
                if ti and ws[k]["l"] - margin[half_of(ws[k]["l"])] <= 0.13 * width:
                    drop.update(range(k, k + ti[0]))
                    break
            body_ws = [w for i, w in enumerate(ws) if i not in drop]
            if not body_ws:
                continue
            is_title, icons, body_text, title_left = False, 0, "", 0
            for j in range(0, min(7, len(body_ws))):
                cand = body_ws[j:]
                cand_text = " ".join(w["t"] for w in cand)
                head = re.split(r"[—–]|-(?=[A-Z][a-z])|\(", cand_text)[0]
                first = cand[0]
                if (upper_ratio(head) >= 0.8 and sum(ch.isalpha() for ch in first["t"]) >= 2
                        and first["conf"] >= 45 and not DUR_RE.search(cand_text) and len(head.strip()) >= 3):
                    is_title, icons, body_text, title_left = True, j, cand_text, first["l"]
                    break
            top = min(w["top"] for w in ws)
            h = half_of(ws[0]["l"])
            if is_title:
                had_color = bool(COLOR_RE.search(body_text))
                body_text = COLOR_RE.sub("", body_text).strip()
                parts = re.split(r"\s*[—–]+\s*|-(?=[A-Z][a-z])", body_text, maxsplit=1)
                title, tag = parts[0].strip(), (parts[1].strip() if len(parts) > 1 else "")
                # a wrapped title continues at the column's left margin; a new program starts after its icons
                if (pending and pending["_wrap"] and not pending["desc"] and not pending["tag"]
                        and pending["_half"] == h and title_left <= pending["_row_left"] + 0.012 * width):
                    joiner = "" if pending["title"].endswith("-") else " "
                    pending["title"] = (pending["title"].rstrip("-") + joiner + title).strip()
                    pending["tag"] = tag
                    pending["_wrap"] = False
                    continue
                if pending:
                    recs_page.append(pending)
                pending = {"issue": issue, "day": day, "page": page_no, "half": h, "time": time_for(h, top),
                           "title": title, "tag": tag, "icons": icons, "minutes": None, "desc": "",
                           "_wrap": (not tag and not had_color) or title.endswith("-"),
                           "_half": h, "_row_left": ws[0]["l"]}
                continue
            if pending and pending["_half"] == h:
                pending["desc"] += " " + text
                pending["_wrap"] = False
                d = DUR_RE.search(text)
                if d and pending["minutes"] is None:
                    pending["minutes"] = (int(d.group(1)) if d.group(1)
                                          else WORDNUM[d.group(2).lower()] if d.group(2) else int(d.group(3)) * 60)
        if pending:
            recs_page.append(pending)

        records.extend(recs_page)
        prev_last_time = last_abs

    out = []
    for r in records:
        title = re.sub(r"\s+", " ", r["title"]).strip()
        if title.upper() in JUNK_TITLES or len(title) < 3 or re.match(r"(?i)^(january|february|march|april|may|june|july|august|september|october|november|december)\b.*\d", title):
            continue
        r["title"] = title
        r["desc"] = r["desc"].strip()[:300]
        r.pop("_wrap", None)
        r.pop("_half", None)
        r.pop("_row_left", None)
        out.append(r)
    return out


def fmt(t):
    return f"{t // 60 % 24:02d}:{t % 60:02d}" if t is not None else "  ?  "


if __name__ == "__main__":
    import sys
    recs = parse_issue(sys.argv[1], sys.argv[2])
    want_day = sys.argv[3] if len(sys.argv) > 3 else None
    for r in recs:
        if want_day and r["day"] != want_day:
            continue
        if r["time"] is not None and r["time"] > 14 * 60 + 30:
            continue
        print(r["day"][:3], fmt(r["time"]), f'ic={r["icons"]}', r["title"][:36], "|", r["tag"][:12], "|", r["minutes"])

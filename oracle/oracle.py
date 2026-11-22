#!/usr/bin/env python3
"""Slow, obviously-correct reference implementation of rules.md (test oracle only).

Usage: oracle.py [DATA_DIR OUTPUT_DIR]   (or env DATA_DIR / OUTPUT_DIR)
"""
import bisect
import datetime
import glob
import json
import os
import re
import sys

import pyarrow as pa
import pyarrow.parquet as pq

DAY = 86400000
F1 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z$")
F2 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})$")
F3 = re.compile(r"^[0-9]+$")
DIG = re.compile(r"^[0-9]+$")
EPOCH = datetime.date(1970, 1, 1)


def read_lines(path):
    with open(path, "rb") as f:
        data = f.read().decode("utf-8", errors="surrogateescape")
    lines = data.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines[1:]  # header


def split_fields(line, n):
    parts = line.split(",")
    if len(parts) != n:
        return None
    out = []
    for p in parts:
        if '"' in p:
            if len(p) >= 2 and p[0] == '"' and p[-1] == '"' and '"' not in p[1:-1]:
                p = p[1:-1]
            else:
                return None
        out.append(p)
    return out


def to_ms(parts):
    y, mo, d, h, mi, s = (int(x) for x in parts[:6])
    if not (1 <= y <= 9999 and 1 <= mo <= 12 and 0 <= h <= 23 and 0 <= mi <= 59 and 0 <= s <= 59):
        return None
    try:
        dd = datetime.date(y, mo, d)
    except ValueError:
        return None
    return ((dd - EPOCH).days * DAY) + ((h * 60 + mi) * 60 + s) * 1000


def parse_ts(s):
    m = F1.match(s)
    if m:
        return to_ms(m.groups())
    m = F2.match(s)
    if m:
        g = m.groups()
        base = to_ms(g[:6])
        oh, om = int(g[7]), int(g[8])
        if base is None or oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60000
        return base - off if g[6] == "+" else base + off
    if F3.match(s):
        return int(s)
    return None


class UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        p = self.p
        if x not in p:
            p[x] = x
            return x
        r = x
        while p[r] != r:
            r = p[r]
        while p[x] != r:
            p[x], x = r, p[x]
        return r

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if rb < ra:
            ra, rb = rb, ra
        self.p[rb] = ra  # root = smallest


def main():
    data = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DATA_DIR", "/data")
    out = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("OUTPUT_DIR", "/output")
    man = json.load(open(os.path.join(data, "manifest.json")))

    def iso(s):
        return parse_ts(s)

    ws, we = iso(man["window_start"]), iso(man["window_end"])

    cat = {}
    for line in read_lines(os.path.join(data, "products.csv")):
        f = split_fields(line, 2)
        if f:
            cat[f[0]] = f[1]
    chan = {}
    for line in read_lines(os.path.join(data, "campaigns.csv")):
        f = split_fields(line, 2)
        if f:
            chan[f[0]] = f[1]

    uf = UF()
    for path in sorted(glob.glob(os.path.join(data, "links", "*.csv"))):
        for line in read_lines(path):
            f = split_fields(line, 2)
            if not f or f[0] == "" or f[1] == "":
                continue
            uf.union(f[0], f[1])

    # events
    best = {}
    for path in sorted(glob.glob(os.path.join(data, "events", "*.csv"))):
        for line in read_lines(path):
            f = split_fields(line, 7)
            if not f or f[0] == "":
                continue
            ing = parse_ts(f[3])
            key = f[0]
            if key not in best or (ing is not None and (best[key][0] is None or ing < best[key][0])):
                best[key] = (ing, f)
    per = {}
    for eid, (ing, f) in best.items():
        _, uid, ts, _, et, pid, price = f
        if uid == "" or pid == "" or et not in ("view", "cart", "purchase"):
            continue
        t = parse_ts(ts)
        if t is None or not (ws <= t < we):
            continue
        rev = 0
        if et == "purchase":
            if not DIG.match(price):
                continue
            rev = int(price)
            if not (1 <= rev <= 10000000):
                continue
        if ing is None or ing - t > DAY:
            continue
        person = uf.find(uid)
        per.setdefault(person, []).append((t, et == "purchase", rev, cat.get(pid, "unknown")))

    # clicks
    seen = {}
    for path in sorted(glob.glob(os.path.join(data, "clicks", "*.csv"))):
        for line in read_lines(path):
            f = split_fields(line, 4)
            if not f or f[0] == "":
                continue
            seen.setdefault(f[0], f)
    clicks = {}
    for cid, (_, uid, camp, ts) in seen.items():
        if uid == "" or camp not in chan:
            continue
        t = parse_ts(ts)
        if t is None or not (ws - 7 * DAY <= t < we):
            continue
        clicks.setdefault(uf.find(uid), []).append((t, cid, camp))
    for v in clicks.values():
        v.sort(key=lambda x: (x[0], x[1].encode("utf-8", "surrogateescape")))

    ud = {}  # (person, day) -> [sessions, events, rev, {cat: sum}]
    cd = {}  # (campaign, day) -> [sessions, purch, rev, assisted]

    def cdrow(c, d):
        return cd.setdefault((c, d), [0, 0, 0, 0])

    for person, evs in per.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        cur = None
        prev = None
        for e in evs:
            if prev is None or e[0] - prev > 1800000:
                cur = [e[0], 0, 0, 0, {}]
                sessions.append(cur)
            prev = e[0]
            cur[1] += 1
            if e[1]:
                cur[2] += 1
                cur[3] += e[2]
                cur[4][e[3]] = cur[4].get(e[3], 0) + e[2]
        cl = clicks.get(person, [])
        tss = [c[0] for c in cl]
        for st, n, npur, rev, cats in sessions:
            day = st // DAY
            u = ud.setdefault((person, day), [0, 0, 0, {}])
            u[0] += 1
            u[1] += n
            u[2] += rev
            for k, v in cats.items():
                u[3][k] = u[3].get(k, 0) + v
            lo = bisect.bisect_left(tss, st - 7 * DAY)
            hi = bisect.bisect_right(tss, st)
            if lo < hi:
                # latest ts, tie smallest click_id: sorted by (ts, id); last ts group's first element
                last_ts = tss[hi - 1]
                first = bisect.bisect_left(tss, last_ts, lo, hi)
                camp = cl[first][2]
                row = cdrow(camp, day)
                assist = {c[2] for c in cl[lo:hi]}
            else:
                camp = "organic"
                row = cdrow(camp, day)
                assist = set()
            row[0] += 1
            row[1] += 1 if npur else 0
            row[2] += rev
            for a in assist:
                cdrow(a, day)[3] += 1

    def date(d):
        return EPOCH + datetime.timedelta(days=d)

    urows = []
    for (p, d), (s, n, rev, cats) in ud.items():
        top = None
        if cats:
            top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0].encode("utf-8", "surrogateescape")))[0][0]
        urows.append((p, date(d), s, n, rev, top))
    crows = []
    for (c, d), (s, pu, rev, a) in cd.items():
        crows.append((c, "organic" if c == "organic" else chan[c], date(d), s, pu, rev, a))

    def write(name, cols, rows):
        os.makedirs(os.path.join(out, name), exist_ok=True)
        arrays = []
        for i, (cn, ty) in enumerate(cols):
            arrays.append(pa.array([r[i] for r in rows], type=ty))
        t = pa.table(dict(zip([c for c, _ in cols], arrays)))
        pq.write_table(t, os.path.join(out, name, "part-0.parquet"))

    S, D, I = pa.string(), pa.date32(), pa.int64()
    write("user_daily", [("user_id", S), ("day", D), ("sessions", I), ("events", I), ("revenue_cents", I), ("top_category", S)], urows)
    write("campaign_daily", [("campaign_id", S), ("channel", S), ("day", D), ("sessions", I), ("purchasing_sessions", I), ("revenue_cents", I), ("assisted_sessions", I)], crows)


if __name__ == "__main__":
    main()

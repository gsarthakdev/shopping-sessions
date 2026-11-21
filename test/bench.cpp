// Single-threaded per-line cost breakdown of the event parse path.
// usage: bench FILE.csv
#define main real_main
#include "../src/main.cpp"
#undef main

#ifndef TSFN
#define TSFN parse_ts
#endif
static double now() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

int main(int argc, char** argv) {
  File f = map_file(argv[1]);
  const char* b = (const char*)memchr(f.data, '\n', f.size) + 1;
  const char* e = f.data + f.size;
  while (e[-1] != '\n') e--;
  // warm the page cache mapping
  volatile u64 sink = 0;
  for (const char* p = b; p < e; p += 4096) sink = sink + *p;
  size_t lines = 0;
  scan_lines<7>(b, e, [&](Str*) { lines++; });
  printf("%zu lines, %.1f MB\n", lines, (e - b) / 1e6);
  const int REPS = getenv("REPS") ? atoi(getenv("REPS")) : 5;
  auto run = [&](const char* name, auto&& body) {
    double best = 1e9;
    u64 acc = 0;
    for (int rep = 0; rep < REPS; rep++) {
      double t = now();
      acc = 0;
      for (const char* p = b; p < e;) {
        const char* q = p + (256 << 10);
        if (q >= e) q = e; else q = (const char*)memchr(q, '\n', e - q) + 1;
        scan_lines2<7>(p, q, [&](Str* fl) { acc += body(fl); });
        p = q;
      }
      best = std::min(best, now() - t);
    }
    printf("%-28s %6.1f ns/line  (%llu)\n", name, best * 1e9 / lines, (unsigned long long)acc);
  };
  run("tokenize", [&](Str* fl) -> u64 { return fl[0].n + fl[6].n; });
  {
    double t = now();
    u64 acc = 0;
    for (const char* p = b; p < e;) {
      const char* q = p + (256 << 10);
      if (q >= e) q = e; else q = (const char*)memchr(q, '\n', e - q) + 1;
      scan_lines2<7>(p, q, [&](Str* fl) { acc += fl[0].n + fl[6].n; });
      p = q;
    }
    t = now() - t;
    printf("%-28s %6.1f ns/line  (%llu)\n", "tokenize2 (256K chunks)", t * 1e9 / lines, (unsigned long long)acc);
  }
  run("+ ingest", [&](Str* fl) -> u64 {
    i64 c = 0;
    parse_ingest_s(fl[3].p, fl[3].n, c);
    return (u64)c;
  });
  run("+ ts only", [&](Str* fl) -> u64 {
    i64 a = 0;
    TSFN(fl[2].p, fl[2].n, a);
    return (u64)a;
  });
  run("+ ts parse", [&](Str* fl) -> u64 {
    i64 a = 0, c = 0;
    TSFN(fl[2].p, fl[2].n, a);
    parse_ingest_s(fl[3].p, fl[3].n, c);
    return (u64)(a + c);
  });
  run("+ eid hash2", [&](Str* fl) -> u64 {
    i64 a = 0, c = 0;
    TSFN(fl[2].p, fl[2].n, a);
    parse_ingest_s(fl[3].p, fl[3].n, c);
    u32 h2;
    return (u64)(a + c) + hbytes2(fl[0].p, fl[0].n, h2) + h2;
  });
  run("+ uid hash", [&](Str* fl) -> u64 {
    i64 a = 0, c = 0;
    TSFN(fl[2].p, fl[2].n, a);
    parse_ingest_s(fl[3].p, fl[3].n, c);
    u32 h2;
    return (u64)(a + c) + hbytes2(fl[0].p, fl[0].n, h2) + h2 + hbytes(fl[1].p, fl[1].n);
  });
  // full validation, then + intern, then + push
  string dir = string(argv[1]).substr(0, string(argv[1]).rfind("/events/"));
  string prod_s = read_all(dir + "/products.csv");
  vector<Str> pkeys;
  {
    const char* p = prod_s.data();
    const char* pe = p + prod_s.size();
    p = (const char*)memchr(p, '\n', pe - p) + 1;
    scan_lines<2>(p, pe, [&](Str* fl) { pkeys.push_back(fl[0]); });
  }
  LookupTable prodT;
  prodT.build(pkeys);
  const i64 WS = 0, WE = INT64_MAX / 2;
  auto validate = [&](Str* f, EvRec& r, const char*& up, u32& un) -> bool {
    u64 h = hbytes2(f[0].p, f[0].n, r.h2);
    r.hlo = (u32)h;
    i64 ings = 0;
    parse_ingest_s(f[3].p, f[3].n, ings);
    r.ing = (u32)ings;
    i64 ing = ings * 1000;
    r.pc = 0;
    i64 ts;
    if (f[1].n && f[5].n && TSFN(f[2].p, f[2].n, ts) && ts >= WS && ts < WE && ing - ts <= DAYMS) {
      u32 pc = 0;
      const char* tp = f[4].p;
      if (f[4].n == 4) {
        if (memcmp(tp, "view", 4) == 0 || memcmp(tp, "cart", 4) == 0) pc = 1;
      } else if (f[4].n == 8 && memcmp(tp, "purchase", 8) == 0) {
        int ci2 = prodT.find(f[5].p, f[5].n);
        pc = (1000u << 8) | (u32)(ci2 >= 0 ? ci2 : 0);
      }
      r.pc = pc;
      r.ts_rel = (u32)ts;
      up = f[1].p;
      un = f[1].n;
      return pc != 0;
    }
    return false;
  };
  // the events callback of main(), minus the ID lookup and the record push
  {
    string man = read_all(dir + "/manifest.json");
    auto jget = [&](const char* key) -> i64 {
      size_t q = man.find(key);
      q = man.find(':', q);
      size_t q1 = man.find('"', q), q2 = man.find('"', q1 + 1);
      i64 v = 0;
      parse_ts(man.data() + q1 + 1, (u32)(q2 - q1 - 1), v);
      return v;
    };
    const i64 WS = jget("\"window_start\""), WE = jget("\"window_end\"");
    const int PE_BITS = 10, UNKNOWN = 0;
    run("pipeline (no intern)", [&](Str* f) -> u64 {
      if (!f[0].n) return 0;
      EvRec r;
      const u64 h = hbytes2(f[0].p, f[0].n, r.h2);
      r.hlo = (u32)h;
      const u32 part = (u32)(h >> (64 - PE_BITS));
      i64 ings;
      const bool ingok = parse_ingest_s(f[3].p, f[3].n, ings) && ings >= 0 && ings < (i64)UINT32_MAX;
      r.ing = ingok ? (u32)ings : UINT32_MAX;
      const i64 ing = ings * 1000;
      r.pc = 0;
      r.person = 0;
      r.ts_rel = 0;
      i64 ts;
      u64 uh = 0;
      if (ingok && f[1].n && f[5].n && TSFN(f[2].p, f[2].n, ts) && ts >= WS && ts < WE && ing - ts <= DAYMS) {
        u32 pc = 0;
        const char* tp = f[4].p;
        if (f[4].n == 4) {
          if (memcmp(tp, "view", 4) == 0 || memcmp(tp, "cart", 4) == 0) pc = 1;
        } else if (f[4].n == 8 && memcmp(tp, "purchase", 8) == 0) {
          const char* pp = f[6].p;
          u32 pn = f[6].n;
          bool ok = pn > 0;
          u64 v = 0;
          for (u32 i = 0; ok && i < pn; i++) {
            unsigned d = (unsigned char)pp[i] - '0';
            if (d > 9)
              ok = false;
            else if (v <= 100000000ULL)
              v = v * 10 + d;
          }
          if (ok && v >= 1 && v <= 10000000ULL) {
            int ci2 = prodT.find(f[5].p, f[5].n);
            pc = ((u32)v << 8) | (u32)(ci2 >= 0 ? ci2 : UNKNOWN);
          }
        }
        if (pc) {
          r.pc = pc;
          r.ts_rel = (u32)(ts - WS);
          uh = hbytes(f[1].p, f[1].n);
        }
      }
      return r.hlo + r.h2 + r.ing + r.pc + r.ts_rel + part + uh;
    });
  }
  run("full validate", [&](Str* fl) -> u64 {
    EvRec r;
    const char* up;
    u32 un;
    bool ok = validate(fl, r, up, un);
    return r.hlo + ok;
  });
  if (getenv("QUICK")) return 0;
  IdTable ids;
  ids.init(1 << 22);
  u32 local = 0;
  run("+ intern (no prefetch)", [&](Str* fl) -> u64 {
    EvRec r;
    const char* up;
    u32 un;
    if (validate(fl, r, up, un)) r.person = ids.intern(up, un, local);
    return r.hlo + r.person;
  });
  run("+ intern again (warm)", [&](Str* fl) -> u64 {
    EvRec r;
    const char* up;
    u32 un;
    if (validate(fl, r, up, un)) r.person = ids.intern(up, un, local);
    return r.hlo + r.person;
  });
  for (int PB : {8, 16, 32, 64}) {
    struct Pend {
      EvRec r;
      const char* up;
      u32 un;
      u64 uh;
    };
    vector<Pend> pend(PB);
    int np = 0;
    u64 acc2 = 0;
    auto flush = [&]() {
      for (int i = 0; i < np; i++) { u32 sl = ids.intern_h(pend[i].up, pend[i].un, pend[i].uh, local); acc2 += ids.person_of(sl); }
      np = 0;
    };
    char nm[64];
    snprintf(nm, sizeof nm, "+ intern prefetch PB=%d", PB);
    run(nm, [&](Str* fl) -> u64 {
      EvRec r;
      const char* up;
      u32 un;
      if (validate(fl, r, up, un)) {
        Pend& p = pend[np++];
        p.r = r;
        p.up = up;
        p.un = un;
        p.uh = hbytes(up, un);
        ids.prefetch(p.uh);
        if (np == PB) flush();
      }
      return r.hlo;
    });
    flush();
  }
  vector<BList<EvRec, 500>> parts(256);
  Arena ar;
  ar.init(lines * 40 + (64 << 20));
  run("+ push 256 parts", [&](Str* fl) -> u64 {
    EvRec r;
    const char* up;
    u32 un;
    if (validate(fl, r, up, un)) r.person = ids.intern(up, un, local);
    parts[r.h2 >> 24].push(r, ar);
    return r.hlo;
  });
  return 0;
}

// Included inside a namespace next to a copy of main.cpp: the per-line event work of main()
// (tokenize + field decoding + hashes, no ID lookup / record push) over [b, e).
struct PipeCtx {
  i64 WS, WE;
  const LookupTable* prodT;
};
#ifndef PIPE_TS
#define PIPE_TS parse_ts
#endif
static u64 pipe_events(const char* b, const char* e, const PipeCtx& cx) {
  u64 acc = 0;
  const i64 WS = cx.WS, WE = cx.WE;
  const int PE_BITS = 10, UNKNOWN = 0;
  scan_lines2<7>(b, e, [&](Str* f) {
    if (!f[0].n) return;
    EvRec r;
    const u64 h = hbytes2(f[0].p, f[0].n, r.h2);
    r.hlo = (u32)h;
    const u32 part = (u32)(h >> (64 - PE_BITS));
    i64 ings;
    const bool ingok = parse_ingest_s(f[3].p, f[3].n, ings) && ings >= 0 && ings < (i64)UINT32_MAX;
    r.ing = ingok ? (u32)ings : UINT32_MAX;
    const i64 ing = ings * 1000;
    r.pc = 0;
    r.ts_rel = 0;
    i64 ts;
    u64 uh = 0;
    if (ingok && f[1].n && f[5].n && PIPE_TS(f[2].p, f[2].n, ts) && ts >= WS && ts < WE && ing - ts <= DAYMS) {
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
          int ci2 = cx.prodT->find(f[5].p, f[5].n);
          pc = ((u32)v << 8) | (u32)(ci2 >= 0 ? ci2 : UNKNOWN);
        }
      }
      if (pc) {
        r.pc = pc;
        r.ts_rel = (u32)(ts - WS);
        uh = hbytes(f[1].p, f[1].n);
      }
    }
    acc += r.hlo + r.h2 + r.ing + r.pc + r.ts_rel + part + uh;
  });
  return acc;
}
static u64 pipe_tokenize(const char* b, const char* e) {
  u64 acc = 0;
  scan_lines2<7>(b, e, [&](Str* f) { acc += f[0].n + f[6].n + (u64)(f[3].p - b); });
  return acc;
}
// clicks: the per-line work of main() (no ID lookup / record push)
static u64 pipe_clicks(const char* b, const char* e, i64 CWS, i64 WE, const LookupTable& campT) {
  u64 acc = 0;
  scan_lines2<4>(b, e, [&](Str* f) {
    i64 ts;
    if (f[0].n && f[1].n && parse_ts(f[3].p, f[3].n, ts) && ts >= CWS && ts < WE) {
      int camp = campT.find(f[2].p, f[2].n);
      if (camp >= 0) acc += (u64)ts + (u64)camp + hbytes(f[1].p, f[1].n) + f[0].n;
    }
  });
  return acc;
}

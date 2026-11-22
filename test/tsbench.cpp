// Single-thread microbenchmark of the event-line validation (ts + ingest + type), baseline vs current.
// usage: tsbench EVENTS.csv [WS_ms WE_ms]
#define main real_main
#include "../src/main.cpp"
#undef main
namespace basev {
// Returns false for a malformed timestamp. F3 too large for int64 -> INT64_MAX (outside any window).
__attribute__((always_inline)) static inline bool base_parse_ts(const char* p, u32 n, i64& out) {
  if ((n == 20 || n == 25) && p[4] == '-') {
    u64 a = load8(p) ^ 0x3030303030303030ULL;      // YYYY-MM-
    u64 b = load8(p + 8) ^ 0x3030303030303030ULL;  // DDTHH:MM
    u32 c;
    memcpy(&c, p + 16, 4);  // :SS?
    c ^= 0x30303030u;
    // separators: '-'^'0'=0x1D, 'T'^'0'=0x64, ':'^'0'=0x0A
    if ((a & 0xFF0000FF00000000ULL) != 0x1D00001D00000000ULL) return false;
    if ((b & 0x0000FF0000FF0000ULL) != 0x00000A0000640000ULL) return false;
    if ((c & 0xFF) != 0x0A) return false;
    if (!swar_digits(a, 0x00FFFF00FFFFFFFFULL) || !swar_digits(b, 0xFFFF00FFFF00FFFFULL) ||
        !swar_digits((u64)c, 0x0000000000FFFF00ULL))
      return false;
    int y = (int)(a & 0xFF) * 1000 + (int)((a >> 8) & 0xFF) * 100 + (int)((a >> 16) & 0xFF) * 10 + (int)((a >> 24) & 0xFF);
    int mo = (int)((a >> 40) & 0xFF) * 10 + (int)((a >> 48) & 0xFF);
    int d = (int)(b & 0xFF) * 10 + (int)((b >> 8) & 0xFF);
    int h = (int)((b >> 24) & 0xFF) * 10 + (int)((b >> 32) & 0xFF);
    int mi = (int)((b >> 48) & 0xFF) * 10 + (int)((b >> 56) & 0xFF);
    int s = (int)((c >> 8) & 0xFF) * 10 + (int)((c >> 16) & 0xFF);
    if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59) return false;
    i64 days;
    if (y >= TY0 && y < TY1) {
      int ix = (y - TY0) * 16 + mo;
      if (d > g_mt.len[ix]) return false;
      days = g_mt.first[ix] + d - 1;
    } else {
      if (d > dim(y, mo)) return false;
      days = days_since_epoch(y, mo, d);
    }
    i64 off = 0;
    if (n == 20) {
      if (p[19] != 'Z') return false;
    } else {
      char sg = p[19];
      if (sg != '+' && sg != '-') return false;
      int oh, om;
      if (!d2(p + 20, oh) || p[22] != ':' || !d2(p + 23, om)) return false;
      if (oh > 23 || om > 59) return false;
      off = (i64)(oh * 60 + om) * 60;
      if (sg == '-') off = -off;
    }
    out = ((days * 86400 + h * 3600 + mi * 60 + s) - off) * 1000;
    return true;
  }
  if (n == 13) {  // common F3: 13 digits, parsed 8 + 5 with SWAR
    u64 a = load8(p) ^ 0x3030303030303030ULL;
    u64 b = 0;
    memcpy(&b, p + 8, 5);
    b ^= 0x3030303030ULL;
    if (!swar_digits(a, ~0ULL) || !swar_digits(b, 0xFFFFFFFFFFULL)) return false;
    // 8 digits little-endian -> value (classic SWAR reduction)
    a = (a * 10) + (a >> 8);
    a = (((a & 0x000000FF000000FFULL) * (100 + (1000000ULL << 32))) +
         (((a >> 16) & 0x000000FF000000FFULL) * (1 + (10000ULL << 32)))) >> 32;
    u64 v = a;
    for (int i = 0; i < 5; i++) v = v * 10 + ((b >> (8 * i)) & 0xFF);
    out = (i64)v;
    return true;
  }
  if (n == 0) return false;
  u64 v = 0;
  bool big = false;
  for (u32 i = 0; i < n; i++) {
    unsigned c = (unsigned char)p[i] - '0';
    if (c > 9) return false;
    if (!big) {
      v = v * 10 + c;
      if (v > (u64)4e18) big = true;
    }
  }
  out = big ? TS_BAD_BIG : (i64)v;
  return true;
}
// ingest_ts is always valid F1 (rules.md). Fast path: shape check + SWAR digits, returning whole
// seconds since the epoch; anything that doesn't look like F1 goes through the full parser.
__attribute__((always_inline)) static inline bool base_parse_ingest_s(const char* p, u32 n, i64& sec) {
  if (n == 20 && p[19] == 'Z') {
    u64 a = load8(p) ^ 0x3030303030303030ULL;
    u64 b = load8(p + 8) ^ 0x3030303030303030ULL;
    u32 c;
    memcpy(&c, p + 16, 4);
    c ^= 0x30303030u;
    if (((a & 0xFF0000FF00000000ULL) == 0x1D00001D00000000ULL) &
        ((b & 0x0000FF0000FF0000ULL) == 0x00000A0000640000ULL) & ((c & 0xFF) == 0x0A) &
        swar_digits(a, 0x00FFFF00FFFFFFFFULL) & swar_digits(b, 0xFFFF00FFFF00FFFFULL) &
        swar_digits((u64)c, 0x0000000000FFFF00ULL)) {
      int y = (int)(a & 0xFF) * 1000 + (int)((a >> 8) & 0xFF) * 100 + (int)((a >> 16) & 0xFF) * 10 +
              (int)((a >> 24) & 0xFF);
      int mo = (int)((a >> 40) & 0xFF) * 10 + (int)((a >> 48) & 0xFF);
      int d = (int)(b & 0xFF) * 10 + (int)((b >> 8) & 0xFF);
      int h = (int)((b >> 24) & 0xFF) * 10 + (int)((b >> 32) & 0xFF);
      int mi = (int)((b >> 48) & 0xFF) * 10 + (int)((b >> 56) & 0xFF);
      int s = (int)((c >> 8) & 0xFF) * 10 + (int)((c >> 16) & 0xFF);
      if (y >= TY0 && y < TY1 && mo >= 1 && mo <= 12 && d >= 1 && d <= g_mt.len[(y - TY0) * 16 + mo] && h <= 23 &&
          mi <= 59 && s <= 59) {
        sec = (i64)(g_mt.first[(y - TY0) * 16 + mo] + d - 1) * 86400 + h * 3600 + mi * 60 + s;
        return true;
      }
    }
  }
  i64 ms;
  if (!base_parse_ts(p, n, ms)) return false;
  sec = ms >= 0 ? ms / 1000 : -((-ms + 999) / 1000);
  return true;
}

}
static double now() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
struct L { Str f[7]; };
int main(int argc, char** argv) {
  File fl = map_file(argv[1]);
  const char* b = (const char*)memchr(fl.data, '\n', fl.size) + 1;
  const char* e = fl.data + fl.size;  // (reassigned for SORT)
  while (e[-1] != '\n') e--;
  vector<L> lines;
  scan_lines2<7>(b, e, [&](Str* f) { L l; memcpy(l.f, f, sizeof l.f); lines.push_back(l); });
  const i64 WS = argc > 2 ? atoll(argv[2]) : 1828483200000LL, WE = argc > 3 ? atoll(argv[3]) : 1829692800000LL;
  string sorted;  // SORT=1: same lines, contiguous, grouped by ts length (predictable format branches)
  if (getenv("SORT")) {
    std::stable_sort(lines.begin(), lines.end(), [](const L& x, const L& y) { return x.f[2].n < y.f[2].n; });
    for (const L& l : lines) {
      const char* s0 = l.f[0].p;
      if (s0 > fl.data && s0[-1] == '"') s0--;
      const char* s1 = (const char*)memchr(l.f[6].p, '\n', e - l.f[6].p) + 1;
      sorted.append(s0, s1 - s0);
    }
    sorted.append(64, '\n');
    lines.clear();
    scan_lines2<7>(sorted.data(), sorted.data() + sorted.size() - 64, [&](Str* f) { L l; memcpy(l.f, f, sizeof l.f); lines.push_back(l); });
    e = sorted.data() + sorted.size() - 64;
  }
  const char* const ce = e;
  printf("%zu lines\n", lines.size());
  const char* only = getenv("ONLY");  // run just this variant (for perf stat)
  auto run = [&](const char* name, auto&& body) {
    if (only && strcmp(only, name)) return (u64)0;
    double best = 1e9;
    u64 acc = 0;
    for (int rep = 0; rep < 7; rep++) {
      double t = now();
      acc = 0;
      for (const L& l : lines) acc += body(l.f);
      best = std::min(best, now() - t);
    }
    printf("%-10s %6.2f ns/line  (%llu)\n", name, best * 1e9 / lines.size(), (unsigned long long)acc);
    return acc;
  };
  for (int round = 0; round < 2; round++) {
  u64 r0 = run("base", [&](const Str* f) -> u64 {
    using namespace basev;
    i64 ings;
    const bool ingok = base_parse_ingest_s(f[3].p, f[3].n, ings) && ings >= 0 && ings < (i64)UINT32_MAX;
    u64 ring = ingok ? (u32)ings : UINT32_MAX;
    const i64 ing = ings * 1000;
    i64 ts;
    u64 pc = 0, rel = 0;
    if (ingok && f[1].n && f[5].n && base_parse_ts(f[2].p, f[2].n, ts) && ts >= WS && ts < WE && ing - ts <= DAYMS) {
      const char* tp = f[4].p;
      if (f[4].n == 4) {
        if (memcmp(tp, "view", 4) == 0 || memcmp(tp, "cart", 4) == 0) pc = 1;
      } else if (f[4].n == 8 && memcmp(tp, "purchase", 8) == 0) {
        pc = 2;
      }
      rel = pc ? (u64)(ts - WS) : 0;
    }
    return ring * 3 + pc * 7 + rel;
  });
  u64 r1 = run("new", [&](const Str* f) -> u64 {
    i64 ings = 0;
    const bool ingok = parse_ingest_s(f[3].p, f[3].n, ings) & (ings >= 0) & (ings < (i64)UINT32_MAX);
    u64 ring = ingok ? (u32)ings : UINT32_MAX;
    const i64 ing = ings * 1000;
    i64 ts = 0;
    const bool tsok = parse_ts_fast(f[2].p, f[2].n, ce, ts);
    const char* tp = f[4].p;
    const u32 tn = f[4].n;
    u64 tw = 0;
    if (__builtin_expect(tp + 8 <= ce, 1))
      tw = load8(tp);
    else
      memcpy(&tw, tp, tn < 8 ? tn : 8);
    const bool vc = (tn == 4) & (((u32)tw == TYPE_VIEW) | ((u32)tw == TYPE_CART));
    const bool pu = (tn == 8) & (tw == TYPE_PURCHASE);
    u64 pc = 0, rel = 0;
    if (ingok & (f[1].n != 0) & (f[5].n != 0) & tsok & (ts >= WS) & (ts < WE) & (ing - ts <= DAYMS) & (vc | pu)) {
      pc = pu ? 2 : 1;
      rel = (u64)(ts - WS);
    }
    return ring * 3 + pc * 7 + rel;
  });
  u64 r2 = run("ts-base", [&](const Str* f) -> u64 { i64 ts = 0; return basev::base_parse_ts(f[2].p, f[2].n, ts) + (u64)ts; });
  u64 r3 = run("ts-new", [&](const Str* f) -> u64 { i64 ts = 0; return parse_ts_fast(f[2].p, f[2].n, ce, ts) + (u64)ts; });
  run("touch", [&](const Str* f) -> u64 { return f[2].n + (u8)f[2].p[0] + (u8)f[3].p[0]; });
  u64 r4 = run("ing-base", [&](const Str* f) -> u64 { i64 s = 0; return basev::base_parse_ingest_s(f[3].p, f[3].n, s) + (u64)s; });
  u64 r5 = run("ing-new", [&](const Str* f) -> u64 { i64 s = 0; return parse_ingest_s(f[3].p, f[3].n, s) + (u64)s; });
  if (r0 != r1) printf("RESULT MISMATCH full\n");
  (void)r2; (void)r3;
  if (r4 != r5) printf("RESULT MISMATCH ingest\n");
  }
  return 0;
}

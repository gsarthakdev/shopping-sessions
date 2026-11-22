// Fuzz the fast timestamp parsers (parse_ts, parse_ts_fast, parse_ingest_s) against the
// straightforward one.
#define main real_main
#include "../src/main.cpp"
#undef main
#include <random>

static long g_checked = 0, g_valid = 0;
// Check every fast parser on the n-byte field at p (bytes after it are arbitrary; [p, lim) readable).
static bool check(const char* p, u32 n, const char* lim) {
  i64 a = 0, b = 0;
  bool ra = parse_ts(p, n, a), rb = parse_ts_slow(p, n, b);
  if (ra != rb || (ra && a != b)) {
    fprintf(stderr, "MISMATCH '%.*s' (n=%u) fast=%d/%lld slow=%d/%lld\n", (int)n, p, n, ra, (long long)a, rb,
            (long long)b);
    return false;
  }
  i64 f = 0;
  bool rf = parse_ts_fast(p, n, lim, f);
  if (rf != rb || (rf && f != b)) {
    fprintf(stderr, "FAST MISMATCH '%.*s' (n=%u) fast=%d/%lld slow=%d/%lld\n", (int)n, p, n, rf, (long long)f, rb,
            (long long)b);
    return false;
  }
  i64 sec = 0;
  bool ri = parse_ingest_s(p, n, sec);  // always called: must stay in bounds on any input
  i64 want = rb ? (b >= 0 ? b / 1000 : -((-b + 999) / 1000)) : 0;
  // unchecked: ingest_ts is always valid F1, so only valid F1 input has a defined result
  const bool cmp_ingest = rb && n == 20 && p[19] == 'Z';
  if (cmp_ingest && (ri != rb || (ri && sec != want))) {
    fprintf(stderr, "INGEST MISMATCH '%.*s'\n", (int)n, p);
    return false;
  }
  g_checked++;
  g_valid += rb;
  return true;
}

// Expected verdicts for the forms rules.md spells out (and boundaries around them).
struct Case {
  const char* s;
  int ok;        // 1 valid, 0 bad
  long long ms;  // expected value when valid (ignored if -1)
};
static const Case CASES[] = {
    {"2026-10-03T23:58:10Z", 1, 1791071890000LL},
    {"2026-10-03T19:58:10-04:00", 1, 1791071890000LL},
    {"1791071890000", 1, 1791071890000LL},
    {"0001791071890000", 1, 1791071890000LL},
    {"00000001791071890000", 1, 1791071890000LL},       // 20 digits
    {"0000000000001791071890000", 1, 1791071890000LL},  // 25 digits
    {"86400000", 1, 86400000LL},
    {"0", 1, 0},
    {"0000000000000", 1, 0},
    {"9999999999999", 1, 9999999999999LL},
    {"99999999999999999999", 1, INT64_MAX},  // too long for int64: valid, outside any window
    {"99999999999999999999999999999999999999", 1, INT64_MAX},
    {"2026-10-03T23:58:10+00:00", 1, 1791071890000LL},
    {"2026-10-03T23:58:10-00:00", 1, 1791071890000LL},
    {"2026-10-04T05:28:10+05:30", 1, 1791071890000LL},
    {"2026-10-03T13:58:10-10:00", 1, 1791071890000LL},
    {"2026-10-04T11:58:10+12:00", 1, 1791071890000LL},
    {"2026-10-04T00:00:00+23:59", 1, -1},
    {"2026-10-03T23:59:59-23:59", 1, -1},
    {"2028-02-29T12:00:00Z", 1, -1},
    {"2000-02-29T00:00:00Z", 1, -1},
    {"2028-02-29T23:00:00-05:00", 1, -1},  // date changes with the offset
    {"2028-03-01T01:00:00+05:00", 1, -1},
    {"2027-12-31T23:59:59Z", 1, -1},
    {"2028-01-01T00:00:00+00:01", 1, -1},
    {"1970-01-01T00:00:00Z", 1, 0},
    {"1969-12-31T23:59:59Z", 1, -1000},
    {"1899-12-31T23:59:59Z", 1, -1},  // outside the month table
    {"1900-01-01T00:00:00Z", 1, -1},
    {"2199-12-31T23:59:59Z", 1, -1},
    {"2200-01-01T00:00:00Z", 1, -1},
    {"0000-01-01T00:00:00Z", 1, -1},
    {"9999-12-31T23:59:59Z", 1, -1},
    {"2026-10-03T23:58:10+05:30", 1, -1},
    // bad
    {"", 0, 0},
    {"2026-10-03T23:58:10.000Z", 0, 0},
    {"2026-10-03T23:58:10.5Z", 0, 0},
    {"2026-10-03T23:58:10z", 0, 0},
    {"2026-10-03t23:58:10Z", 0, 0},
    {"2026-10-03 23:58:10Z", 0, 0},
    {"2026-10-03T23:58:10", 0, 0},
    {"2026-10-03T23:58:10+0200", 0, 0},
    {"2026-10-03T23:58:10+02", 0, 0},
    {"2026-10-03T23:58:10+24:00", 0, 0},
    {"2026-10-03T23:58:10-24:00", 0, 0},
    {"2026-10-03T23:58:10+05:60", 0, 0},
    {"2026-10-03T23:58:10*05:30", 0, 0},
    {"2026-10-03T23:58:10+05-30", 0, 0},
    {"2026-10-03T23:58:10Z05:30", 0, 0},
    {"2026-10-03T23:58:10+5:30", 0, 0},
    {"2026-10-03T23:58:10+05:3", 0, 0},
    {"2026-10-03T23:58:10+05:3a", 0, 0},
    {"2026-13-03T23:58:10Z", 0, 0},
    {"2026-00-03T23:58:10Z", 0, 0},
    {"2026-10-00T23:58:10Z", 0, 0},
    {"2026-10-32T23:58:10Z", 0, 0},
    {"2026-04-31T23:58:10Z", 0, 0},
    {"2026-02-30T00:00:00Z", 0, 0},
    {"2027-02-29T00:00:00Z", 0, 0},
    {"1900-02-29T00:00:00Z", 0, 0},
    {"2100-02-29T00:00:00Z", 0, 0},
    {"2026-10-03T24:00:00Z", 0, 0},
    {"2026-10-03T23:60:00Z", 0, 0},
    {"2026-10-03T23:58:60Z", 0, 0},
    {"2026-10-03T23:58:10ZZ", 0, 0},
    {"2026-10-03T23:58:1Z", 0, 0},
    {"2026-1-03T23:58:10Z", 0, 0},
    {"26-10-03T23:58:10Z", 0, 0},
    {"2026/10/03T23:58:10Z", 0, 0},
    {"2026-10-03T23-58-10Z", 0, 0},
    {"2026-10-03T23:58:10-04:00 ", 0, 0},
    {" 2026-10-03T23:58:10Z", 0, 0},
    {"+2026-10-03T23:58:10Z", 0, 0},
    {"1791071890000.0", 0, 0},
    {"1.79e12", 0, 0},
    {"1.79e1200000", 0, 0},
    {"-5", 0, 0},
    {"+5", 0, 0},
    {"-791071890000", 0, 0},
    {" 791071890000", 0, 0},
    {"179107189000 ", 0, 0},
    {"17910718900a0", 0, 0},
    {"1791071890000Z", 0, 0},
    {"0x1791071890", 0, 0},
    {"2026-10-03T23:58:10Z0000", 0, 0},
    {"00000001791071890000Z", 0, 0},
    {"2026010323581000000", 1, 2026010323581000000LL},
    {"20260103235810000000", 1, INT64_MAX},
    {"18446745903709551616", 1, INT64_MAX},  // > 2^64: must not wrap into the window
    {"18446744073709551615", 1, INT64_MAX},
    {"9223372036854775807", 1, INT64_MAX},
    {"9223372036854775808", 1, INT64_MAX},
    {"9223372036854775806", 1, 9223372036854775806LL},
    {"00009223372036854775806", 1, 9223372036854775806LL},
    {"922337203685477580", 1, 922337203685477580LL},
    {"\xb9\xb9\xb9\xb9-10-03T23:58:10Z", 0, 0},
    {"2026-10-03T23:58:10\xff", 0, 0},
    {"2026-10-03T23:58:\xff\xff\x5a", 0, 0},
    {"\xff\x30\x30\x30\x30\x30\x30\x30\x30\x30\x30\x30\x30", 0, 0},
    {"000000000000\xff", 0, 0},
    {"/000000000000", 0, 0},
    {":000000000000", 0, 0},
};

int main() {
  std::mt19937_64 rng(42);
  const char* alpha = "0123456789-T:Z+-zt .9,\"\n";
  // a field that ends exactly at a page whose successor is unmapped
  const size_t PG = 4096;
  char* guard = (char*)mmap(nullptr, 2 * PG, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  mprotect(guard + PG, PG, PROT_NONE);
  int nbad = 0;
  for (const Case& cs : CASES) {
    const u32 n = (u32)strlen(cs.s);
    for (int where = 0; where < 3; where++) {  // roomy buffer / at the page end / 1..40 bytes before it
      char buf[96];
      memset(buf, where ? '9' : ',', sizeof buf);
      char* p = buf;
      const char* lim = buf + sizeof buf;
      if (where) {
        p = guard + PG - n - (where == 2 ? 1 + rng() % 40 : 0);
        lim = guard + PG;
      }
      memcpy(p, cs.s, n);
      i64 v = 0, w = 0, fv = 0;
      bool r = parse_ts(p, n, v), rs = parse_ts_slow(p, n, w), rf = parse_ts_fast(p, n, lim, fv);
      if (r != (bool)cs.ok || rs != (bool)cs.ok || rf != (bool)cs.ok ||
          (cs.ok && cs.ms != -1 && (v != cs.ms || w != cs.ms || fv != cs.ms)) || (cs.ok && (v != w || fv != w))) {
        fprintf(stderr, "CASE '%s': want %d/%lld, fast %d/%lld slow %d/%lld fastlim %d/%lld\n", cs.s, cs.ok, cs.ms, r,
                (long long)v, rs, (long long)w, rf, (long long)fv);
        nbad++;
      }
      if (!check(p, n, lim)) nbad++;
    }
  }
  if (nbad) return 1;
  printf("cases: %zu ok\n", sizeof CASES / sizeof CASES[0]);
  const long ITERS = getenv("ITERS") ? atol(getenv("ITERS")) : 40000000;
  for (long it = 0; it < ITERS; it++) {
    char buf[96];
    for (int i = 0; i < 96; i++)  // what follows the field: CSV-ish noise or arbitrary bytes
      buf[i] = (rng() % 2) ? alpha[rng() % strlen(alpha)] : (char)(rng() % 256);
    int n;
    int form = rng() % 4;
    char tmp[64];
    if (form == 0) {  // F3-ish: mostly 13 digits, sometimes other lengths (20/25 too) / leading zeros
      int len = (rng() % 4) ? 13 : (rng() % 3 == 0) ? (rng() % 2 ? 20 : 25) : (int)(1 + rng() % 30);
      for (int i = 0; i < len; i++) tmp[i] = (char)('0' + rng() % 10);
      if (rng() % 4 == 0)
        for (int z = (int)(rng() % (len + 1)), i = 0; i < z; i++) tmp[i] = '0';
      n = len;
    } else {  // start from a valid-looking F1/F2 timestamp
      int y;
      switch (rng() % 4) {
        case 0: y = (int)(2024 + rng() % 6); break;
        case 1: y = (int)(1880 + rng() % 340); break;
        case 2: y = (int)("\x00\x01\x02\x03"[rng() % 4]) + (rng() % 2 ? 1898 : 2198); break;
        default: y = (int)(rng() % 10000);
      }
      bool tight = rng() % 2;  // mostly-valid ranges vs anything
      int mo = tight ? 1 + rng() % 12 : rng() % 14, d = tight ? 1 + rng() % 31 : rng() % 33,
          h = tight ? rng() % 24 : rng() % 26, mi = tight ? rng() % 60 : rng() % 62,
          s = tight ? rng() % 60 : rng() % 62;
      if (rng() % 8 == 0) mo = 2, d = 28 + rng() % 3;
      int oh = tight ? rng() % 24 : rng() % 26, om = tight ? rng() % 60 : rng() % 62;
      if (rng() % 8 == 0) oh = rng() % 2 ? 23 : 24;
      char sg = "+-"[rng() % 2];
      if (form == 1)
        n = snprintf(tmp, sizeof tmp, "%04d-%02d-%02dT%02d:%02d:%02dZ", y, mo, d, h, mi, s);
      else
        n = snprintf(tmp, sizeof tmp, "%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d", y, mo, d, h, mi, s, sg, oh, om);
    }
    int muts = (rng() % 2) ? 0 : (int)(rng() % 3);
    for (int k = 0; k < muts; k++) {
      int pos = rng() % n;
      tmp[pos] = (rng() % 4 == 0) ? (char)(rng() % 256) : alpha[rng() % strlen(alpha)];
    }
    if (rng() % 16 == 0) {  // insert or delete a byte
      int pos = rng() % (n + 1);
      if (rng() % 2 && n < 40) {
        memmove(tmp + pos + 1, tmp + pos, n - pos);
        tmp[pos] = alpha[rng() % strlen(alpha)];
        n++;
      } else if (n > 0 && pos < n) {
        memmove(tmp + pos, tmp + pos + 1, n - pos - 1);
        n--;
      }
    }
    memcpy(buf, tmp, n);
    if (!check(buf, (u32)n, buf + sizeof buf)) return 1;
    if (it % 64 == 0) {  // near the end of a page: the fast paths must not read past it
      char* p = guard + PG - n;
      memcpy(p, tmp, n);
      if (!check(p, (u32)n, guard + PG)) return 1;
      p = guard + PG - n - (rng() % 40);
      memcpy(p, tmp, n);
      if (!check(p, (u32)n, guard + PG)) return 1;
    }
  }
  printf("ok: %ld checked, %ld valid\n", g_checked, g_valid);
  return 0;
}

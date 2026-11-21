// Fuzz the fast timestamp parsers (parse_ts, parse_ingest_s) against the straightforward one.
#define main real_main
#include "../src/main.cpp"
#undef main
#include <random>

static long g_checked = 0, g_valid = 0;
// Check every fast parser on the n-byte field at p (bytes after it are arbitrary).
static bool check(const char* p, u32 n) {
  i64 a = 0, b = 0;
  bool ra = parse_ts(p, n, a), rb = parse_ts_slow(p, n, b);
  if (ra != rb || (ra && a != b)) {
    fprintf(stderr, "MISMATCH '%.*s' (n=%u) fast=%d/%lld slow=%d/%lld\n", (int)n, p, n, ra, (long long)a, rb,
            (long long)b);
    return false;
  }
  i64 sec = 0;
  bool ri = parse_ingest_s(p, n, sec);
  i64 want = rb ? (b >= 0 ? b / 1000 : -((-b + 999) / 1000)) : 0;
  if (ri != rb || (ri && sec != want)) {
    fprintf(stderr, "INGEST MISMATCH '%.*s'\n", (int)n, p);
    return false;
  }
  g_checked++;
  g_valid += rb;
  return true;
}

int main() {
  std::mt19937_64 rng(42);
  const char* alpha = "0123456789-T:Z+-zt .9,\"\n";
  // a field that ends exactly at a page whose successor is unmapped
  const size_t PG = 4096;
  char* guard = (char*)mmap(nullptr, 2 * PG, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  mprotect(guard + PG, PG, PROT_NONE);
  for (long it = 0; it < 30000000; it++) {
    char buf[96];
    for (int i = 0; i < 64; i++)  // what follows the field: CSV-ish noise or arbitrary bytes
      buf[i] = (rng() % 2) ? alpha[rng() % strlen(alpha)] : (char)(rng() % 256);
    int n;
    int form = rng() % 3;
    char tmp[64];
    if (form == 0) {  // F3-ish, mostly 13 digits, sometimes other lengths / leading zeros
      int len = (rng() % 4) ? 13 : (int)(1 + rng() % 24);
      for (int i = 0; i < len; i++) tmp[i] = (char)('0' + rng() % 10);
      if (rng() % 4 == 0) tmp[0] = '0';
      n = len;
    } else {  // start from a valid-looking F1/F2 timestamp
      int y = (rng() % 2) ? (int)(1880 + rng() % 340) : (int)(rng() % 10000), mo = rng() % 14, d = rng() % 33,
          h = rng() % 26, mi = rng() % 62, s = rng() % 62;
      int oh = rng() % 26, om = rng() % 62;
      char sg = "+-"[rng() % 2];
      if (form == 1)
        n = snprintf(tmp, sizeof tmp, "%04d-%02d-%02dT%02d:%02d:%02dZ", y, mo, d, h, mi, s);
      else
        n = snprintf(tmp, sizeof tmp, "%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d", y, mo, d, h, mi, s, sg, oh, om);
    }
    int muts = rng() % 3;
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
    if (!check(buf, (u32)n)) return 1;
    if (it % 64 == 0) {  // near the end of a page: the fast paths must not read past it
      char* p = guard + PG - n;
      memcpy(p, tmp, n);
      if (!check(p, (u32)n)) return 1;
      p = guard + PG - n - (rng() % 40);
      memcpy(p, tmp, n);
      if (!check(p, (u32)n)) return 1;
    }
  }
  printf("ok: %ld checked, %ld valid\n", g_checked, g_valid);
  return 0;
}

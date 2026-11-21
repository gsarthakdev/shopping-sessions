// Fuzz the fast timestamp parser against the straightforward one.
#define main real_main
#include "../src/main.cpp"
#undef main
#include <random>

int main() {
  std::mt19937_64 rng(42);
  const char* alpha = "0123456789-T:Z+-zt .9";
  long checked = 0, valid = 0;
  for (long it = 0; it < 30000000; it++) {
    char buf[32];
    int n = (rng() % 2) ? 20 : 25;
    if (rng() % 3 == 0) {  // F3-ish
      n = 13;
      snprintf(buf, sizeof buf, "%013llu", (unsigned long long)(rng() % 10000000000000ULL));
      int muts = rng() % 2;
      for (int k = 0; k < muts; k++) buf[rng() % n] = alpha[rng() % strlen(alpha)];
      i64 a = 0, b = 0;
      bool ra = parse_ts(buf, n, a), rb = parse_ts_slow(buf, n, b);
      if (ra != rb || (ra && a != b)) {
        fprintf(stderr, "MISMATCH F3 '%.*s' fast=%d/%lld slow=%d/%lld\n", n, buf, ra, (long long)a, rb, (long long)b);
        return 1;
      }
      valid += ra;
      checked++;
      continue;
    }
    // start from a valid-looking timestamp and mutate
    int y = (rng() % 2) ? (int)(1880 + rng() % 340) : (int)(rng() % 10000), mo = rng() % 14, d = rng() % 33, h = rng() % 26, mi = rng() % 62, s = rng() % 62;
    int oh = rng() % 26, om = rng() % 62;
    char sg = "+-"[rng() % 2];
    if (n == 20)
      snprintf(buf, sizeof buf, "%04d-%02d-%02dT%02d:%02d:%02dZ", y, mo, d, h, mi, s);
    else
      snprintf(buf, sizeof buf, "%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d", y, mo, d, h, mi, s, sg, oh, om);
    int muts = rng() % 3;
    for (int k = 0; k < muts; k++) {
      int pos = rng() % n;
      buf[pos] = (rng() % 4 == 0) ? (char)(rng() % 256) : alpha[rng() % strlen(alpha)];
    }
    i64 a = 0, b = 0;
    bool ra = parse_ts(buf, n, a), rb = parse_ts_slow(buf, n, b);
    checked++;
    if (ra != rb || (ra && a != b)) {
      fprintf(stderr, "MISMATCH '%.*s' fast=%d/%lld slow=%d/%lld\n", n, buf, ra, (long long)a, rb, (long long)b);
      return 1;
    }
    valid += ra;
  }
  printf("ok: %ld checked, %ld valid\n", checked, valid);
  return 0;
}

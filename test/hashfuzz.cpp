// Fuzz hbytes / hbytes2 / streq (word loads, no variable-length memcpy) against the straightforward
// memcpy/memcmp versions, including strings that end right before an unmapped page.
#define main real_main
#include "../src/main.cpp"
#undef main
#include <random>

static u64 ref_hbytes(const char* p, u32 n) {
  u64 h = 0x9E3779B97F4A7C15ULL ^ ((u64)n * 0x100000001b3ULL);
  while (n >= 8) {
    u64 v;
    memcpy(&v, p, 8);
    h = (h ^ v) * 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 29;
    p += 8;
    n -= 8;
  }
  u64 v = 0;
  memcpy(&v, p, n);
  h = (h ^ v) * 0x94d049bb133111ebULL;
  h = fmix(h);
  return h ? h : 1;
}
static u64 ref_hbytes2(const char* p, u32 n, u32& h2) {
  u64 h = 0x9E3779B97F4A7C15ULL ^ ((u64)n * 0x100000001b3ULL);
  u64 g = 0xC2B2AE3D27D4EB4FULL + n;
  while (n >= 8) {
    u64 v = load8(p);
    h = (h ^ v) * 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 29;
    g = (g + v) * 0x9FB21C651E98DF25ULL;
    g = (g << 27) | (g >> 37);
    p += 8;
    n -= 8;
  }
  u64 v = 0;
  memcpy(&v, p, n);
  h = (h ^ v) * 0x94d049bb133111ebULL;
  g = (g + v) * 0x9FB21C651E98DF25ULL;
  h2 = (u32)(fmix(g) >> 32);
  h = fmix(h);
  return h ? h : 1;
}

int main() {
  std::mt19937_64 rng(11);
  const size_t PG = 4096;
  // two guarded regions: [page][PROT_NONE]
  char* r1 = (char*)mmap(nullptr, 2 * PG, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  char* r2 = (char*)mmap(nullptr, 2 * PG, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  mprotect(r1 + PG, PG, PROT_NONE);
  mprotect(r2 + PG, PG, PROT_NONE);
  for (size_t i = 0; i < PG; i++) r1[i] = (char)rng(), r2[i] = (char)rng();
  long checks = 0;
  for (long it = 0; it < 20000000; it++) {
    u32 n = (u32)(rng() % 41);
    if (rng() % 2) n = (u32)(rng() % 18);
    // place the string at the very end of the page, or near it, or anywhere
    size_t back = (rng() % 3 == 0) ? n : n + (rng() % (rng() % 2 ? 24 : PG - 64));
    if (back > PG) back = PG;
    char* a = r1 + PG - back;
    for (u32 k = 0; k < n; k++) a[k] = (rng() % 2) ? (char)('a' + rng() % 3) : (char)rng();
    u32 x2, y2;
    if (hbytes(a, n) != ref_hbytes(a, n) || hbytes2(a, n, x2) != ref_hbytes2(a, n, y2) || x2 != y2) {
      fprintf(stderr, "HASH MISMATCH n=%u back=%zu\n", n, back);
      return 1;
    }
    // equality: b is a copy (maybe with one byte changed), also near a page end
    size_t backb = (rng() % 3 == 0) ? n : n + (rng() % 40);
    if (backb > PG) backb = PG;
    char* b = r2 + PG - backb;
    memcpy(b, a, n);
    if (n && rng() % 2) b[rng() % n] ^= (char)(1 + rng() % 255);
    u32 bn = (rng() % 8 == 0) ? (u32)(rng() % 41) : n;
    if (bn > backb) bn = n;
    bool want = n == bn && memcmp(a, b, n) == 0;
    if (streq(a, n, b, bn) != want) {
      fprintf(stderr, "STREQ MISMATCH n=%u bn=%u\n", n, bn);
      return 1;
    }
    checks++;
  }
  printf("ok: %ld checks\n", checks);
  return 0;
}

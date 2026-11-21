// Shopping Sessions pipeline: CSV click-stream -> user_daily / campaign_daily Parquet.
// Single binary, no dependencies. See rules.md for the spec this implements.
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

using std::string;
using std::vector;
typedef int64_t i64;
typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef uint8_t u8;

static const i64 DAYMS = 86400000LL;
static const i64 GAPMS = 1800000LL;
static const i64 WEEKMS = 7 * DAYMS;

static int NT = 8;
static auto T0 = std::chrono::steady_clock::now();
static void logt(const char* what) {
  double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - T0).count();
  fprintf(stderr, "[%7.3f] %s\n", s, what);
}
[[noreturn]] static void die(const char* msg) {
  fprintf(stderr, "fatal: %s (%s)\n", msg, strerror(errno));
  exit(1);
}

template <class F>
static void parallel(int n, F&& f) {
  std::atomic<int> next{0};
  auto body = [&](int t) {
    for (;;) {
      int i = next.fetch_add(1, std::memory_order_relaxed);
      if (i >= n) break;
      f(i, t);
    }
  };
  vector<std::thread> th;
  for (int t = 1; t < NT; t++) th.emplace_back(body, t);
  body(0);
  for (auto& x : th) x.join();
}

// ---------------------------------------------------------------- hashing
static inline u64 fmix(u64 h) {
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdULL;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53ULL;
  h ^= h >> 33;
  return h;
}
static inline u64 hbytes(const char* p, u32 n) {
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
static inline bool streq(const char* a, u32 an, const char* b, u32 bn) {
  return an == bn && memcmp(a, b, an) == 0;
}
// byte-wise lexicographic
static inline int strcmpb(const char* a, u32 an, const char* b, u32 bn) {
  int c = memcmp(a, b, an < bn ? an : bn);
  if (c) return c;
  return an < bn ? -1 : (an > bn ? 1 : 0);
}

// ---------------------------------------------------------------- CSV
struct Str {
  const char* p;
  u32 n;
};

// Split line [b,e) into exactly N fields per the dialect. False if malformed.
template <int N>
static inline bool split(const char* b, const char* e, Str* f) {
  int k = 0;
  const char* s = b;
  for (;;) {
    const char* c = s;
    int q = 0;
    while (c < e) {
      char ch = *c;
      if (ch == ',') break;
      q += (ch == '"');
      c++;
    }
    if (k == N) return false;
    u32 L = (u32)(c - s);
    if (q) {
      if (q != 2 || L < 2 || s[0] != '"' || s[L - 1] != '"') return false;
      f[k].p = s + 1;
      f[k].n = L - 2;
    } else {
      f[k].p = s;
      f[k].n = L;
    }
    k++;
    if (c == e) break;
    s = c + 1;
  }
  return k == N;
}

// ---------------------------------------------------------------- SIMD tokenizer
#if defined(__AVX2__)
#include <immintrin.h>
static inline void masks64(const char* p, u64& nl, u64& cm, u64& qt) {
  __m256i a = _mm256_loadu_si256((const __m256i*)p);
  __m256i b = _mm256_loadu_si256((const __m256i*)(p + 32));
  auto f = [&](char c) {
    __m256i v = _mm256_set1_epi8(c);
    u64 lo = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v));
    u64 hi = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v));
    return lo | (hi << 32);
  };
  nl = f('\n');
  cm = f(',');
  qt = f('"');
}
#elif defined(__ARM_NEON)
#include <arm_neon.h>
static inline u64 neon_mask(uint8x16_t m0, uint8x16_t m1, uint8x16_t m2, uint8x16_t m3) {
  const uint8x16_t bm = {0x01, 0x02, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80, 0x01, 0x02, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
  uint8x16_t t0 = vandq_u8(m0, bm), t1 = vandq_u8(m1, bm), t2 = vandq_u8(m2, bm), t3 = vandq_u8(m3, bm);
  uint8x16_t s0 = vpaddq_u8(t0, t1), s1 = vpaddq_u8(t2, t3);
  s0 = vpaddq_u8(s0, s1);
  s0 = vpaddq_u8(s0, s0);
  return vgetq_lane_u64(vreinterpretq_u64_u8(s0), 0);
}
static inline void masks64(const char* p, u64& nl, u64& cm, u64& qt) {
  uint8x16_t c0 = vld1q_u8((const u8*)p), c1 = vld1q_u8((const u8*)p + 16), c2 = vld1q_u8((const u8*)p + 32),
             c3 = vld1q_u8((const u8*)p + 48);
  auto f = [&](u8 c) {
    uint8x16_t v = vdupq_n_u8(c);
    return neon_mask(vceqq_u8(c0, v), vceqq_u8(c1, v), vceqq_u8(c2, v), vceqq_u8(c3, v));
  };
  nl = f('\n');
  cm = f(',');
  qt = f('"');
}
#else
static inline void masks64(const char* p, u64& nl, u64& cm, u64& qt) {
  nl = cm = qt = 0;
  for (int i = 0; i < 64; i++) {
    nl |= (u64)(p[i] == '\n') << i;
    cm |= (u64)(p[i] == ',') << i;
    qt |= (u64)(p[i] == '"') << i;
  }
}
#endif

// Scan [b,e) (complete lines). For each well-formed line with exactly N fields, call on(f).
// Quoting rule: total quotes in a line must equal 2 x (fields that start and end with a quote, len>=2).
template <int N, class F>
static inline void scan_lines(const char* b, const char* e, F&& on) {
  const size_t n = (size_t)(e - b);
  u32 fb[N + 1], fe[N + 1];
  int nf = 0;
  size_t fstart = 0;
  u64 qcum = 0, qline0 = 0;
  alignas(64) char tail[64];
  for (size_t blk = 0; blk < n; blk += 64) {
    u64 nl, cm, qt;
    if (blk + 64 <= n) {
      masks64(b + blk, nl, cm, qt);
    } else {
      size_t rem = n - blk;
      memset(tail, 0, 64);
      memcpy(tail, b + blk, rem);
      masks64(tail, nl, cm, qt);
    }
    u64 seps = nl | cm;
    while (seps) {
      int i = __builtin_ctzll(seps);
      seps &= seps - 1;
      size_t pos = blk + i;
      if (nf <= N) {
        fb[nf] = (u32)fstart;
        fe[nf] = (u32)pos;
      }
      nf++;
      fstart = pos + 1;
      if ((nl >> i) & 1) {
        u64 qbefore = qcum + (u64)__builtin_popcountll(qt & ((1ULL << i) - 1));
        u64 ql = qbefore - qline0;
        qline0 = qbefore;
        if (nf == N) {
          Str f[N];
          bool ok = true;
          if (ql == 0) {
            for (int k = 0; k < N; k++) f[k] = Str{b + fb[k], fe[k] - fb[k]};
          } else {
            u64 q2 = 0;
            for (int k = 0; k < N; k++) {
              const char* s = b + fb[k];
              u32 L = fe[k] - fb[k];
              if (L >= 2 && s[0] == '"' && s[L - 1] == '"') {
                f[k] = Str{s + 1, L - 2};
                q2 += 2;
              } else {
                f[k] = Str{s, L};
              }
            }
            ok = (q2 == ql);
          }
          if (ok) on(f);
        }
        nf = 0;
      }
    }
    qcum += (u64)__builtin_popcountll(qt);
  }
}

// ---------------------------------------------------------------- timestamps
static inline i64 days_from_civil(i64 y, unsigned m, unsigned d) {
  y -= m <= 2;
  const i64 era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = (unsigned)(y - era * 400);
  const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + (i64)doe - 719468;
}
static inline bool d2(const char* p, int& v) {
  unsigned a = (unsigned char)p[0] - '0', b = (unsigned char)p[1] - '0';
  if (a > 9 || b > 9) return false;
  v = (int)(a * 10 + b);
  return true;
}
static inline int dim(int y, int m) {
  static const int D[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (m == 2) return ((y % 4 == 0) && (y % 100 != 0 || y % 400 == 0)) ? 29 : 28;
  return D[m];
}
static const i64 TS_BAD_BIG = INT64_MAX;
static inline u64 load8(const char* p) {
  u64 v;
  memcpy(&v, p, 8);
  return v;
}
// x = bytes ^ '0': every byte selected by m must be 0..9
static inline bool swar_digits(u64 x, u64 m) {
  return ((((x + 0x7676767676767676ULL) | x) & 0x8080808080808080ULL) & m) == 0;
}
// Cumulative days before month (non-leap), 1-based month.
static const int CUMD[13] = {0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
static inline bool is_leap(int y) { return (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0); }
static inline i64 days_since_epoch(int y, int m, int d) {
  // valid for any y >= 0 (shifted by one 400-year era so the divisions never see a negative)
  i64 yy = y + 399;
  i64 days = yy * 365 + yy / 4 - yy / 100 + yy / 400 - 719162 - 146097;  // days before Jan 1 of y
  return days + CUMD[m] + (m > 2 && is_leap(y)) + d - 1;
}
static bool parse_ts_slow(const char* p, u32 n, i64& out);
// Table of days-since-epoch for the 1st of each month, and month lengths, for years [1900, 2200).
static const int TY0 = 1900, TY1 = 2200;
struct MonthTable {
  int32_t first[(TY1 - TY0) * 16];
  uint8_t len[(TY1 - TY0) * 16];
  MonthTable() {
    for (int y = TY0; y < TY1; y++)
      for (int m = 1; m <= 12; m++) {
        first[(y - TY0) * 16 + m] = (int32_t)days_since_epoch(y, m, 1);
        len[(y - TY0) * 16 + m] = (uint8_t)dim(y, m);
      }
  }
};
static const MonthTable g_mt;
// Returns false for a malformed timestamp. F3 too large for int64 -> INT64_MAX (outside any window).
static inline bool parse_ts(const char* p, u32 n, i64& out) {
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
static bool parse_ts_slow(const char* p, u32 n, i64& out) {
  if ((n == 20 || n == 25) && p[4] == '-') {
    int y1, y2, mo, d, h, mi, s;
    if (!d2(p, y1) || !d2(p + 2, y2) || !d2(p + 5, mo) || p[7] != '-' || !d2(p + 8, d) || p[10] != 'T' ||
        !d2(p + 11, h) || p[13] != ':' || !d2(p + 14, mi) || p[16] != ':' || !d2(p + 17, s))
      return false;
    int y = y1 * 100 + y2;
    if (mo < 1 || mo > 12 || d < 1 || d > dim(y, mo) || h > 23 || mi > 59 || s > 59) return false;
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
    out = ((days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s) - off) * 1000;
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

// ---------------------------------------------------------------- files
struct File {
  const char* data = nullptr;
  size_t size = 0;
};
static File map_file(const string& path) {
  File f;
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) die(path.c_str());
  struct stat st;
  fstat(fd, &st);
  f.size = st.st_size;
  if (f.size) {
    void* m = mmap(nullptr, f.size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (m == MAP_FAILED) die("mmap");
    f.data = (const char*)m;
  }
  close(fd);
  return f;
}
static vector<string> list_csv(const string& dir) {
  vector<string> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (dirent* e = readdir(d)) {
    string n = e->d_name;
    if (n.size() >= 4 && n[0] != '.' && n.compare(n.size() - 4, 4, ".csv") == 0) out.push_back(dir + "/" + n);
  }
  closedir(d);
  std::sort(out.begin(), out.end());
  return out;
}
static string read_all(const string& path) {
  File f = map_file(path);
  string s(f.data ? f.data : "", f.size);
  if (f.data) munmap((void*)f.data, f.size);
  return s;
}

// A chunk of complete lines: [b, e), every line ends with '\n' and e is just past one.
struct Chunk {
  const char* b;
  const char* e;
  int kind;  // 0 links, 1 events, 2 clicks
};
static vector<char*> g_tails;  // owned copies of final lines lacking '\n'

static void make_chunks(const File& f, int kind, size_t target, vector<Chunk>& out) {
  if (!f.size) return;
  const char* base = f.data;
  const char* end = base + f.size;
  const char* nl = (const char*)memchr(base, '\n', f.size);
  if (!nl) return;  // header only
  const char* p = nl + 1;
  // last newline
  const char* lastnl = end - 1;
  while (lastnl >= p && *lastnl != '\n') lastnl--;
  const char* body_end = (lastnl >= p) ? lastnl + 1 : p;
  while (p < body_end) {
    const char* q = p + target;
    if (q >= body_end) {
      q = body_end;
    } else {
      const char* x = (const char*)memchr(q, '\n', body_end - q);
      q = x ? x + 1 : body_end;
    }
    out.push_back({p, q, kind});
    p = q;
  }
  if (body_end < end) {  // trailing line without newline
    size_t L = end - body_end;
    char* buf = (char*)malloc(L + 16);
    memcpy(buf, body_end, L);
    buf[L] = '\n';
    memset(buf + L + 1, 0, 15);
    g_tails.push_back(buf);
    out.push_back({buf, buf + L + 1, kind});
  }
}

// ---------------------------------------------------------------- small lookup table (read-only)
struct LookupTable {
  vector<u64> h;
  vector<int> v;
  vector<Str> k;
  u64 mask = 0;
  void build(const vector<Str>& keys) {
    u64 cap = 16;
    while (cap < keys.size() * 4) cap <<= 1;
    mask = cap - 1;
    h.assign(cap, 0);
    v.assign(cap, -1);
    k.assign(cap, Str{nullptr, 0});
    for (size_t i = 0; i < keys.size(); i++) {
      u64 hh = hbytes(keys[i].p, keys[i].n);
      u64 idx = hh & mask;
      bool dup = false;
      while (h[idx]) {
        if (h[idx] == hh && streq(k[idx].p, k[idx].n, keys[i].p, keys[i].n)) {
          dup = true;
          break;
        }
        idx = (idx + 1) & mask;
      }
      if (dup) continue;
      h[idx] = hh;
      v[idx] = (int)i;
      k[idx] = keys[i];
    }
  }
  inline int find(const char* p, u32 n) const {
    u64 hh = hbytes(p, n);
    u64 idx = hh & mask;
    while (h[idx]) {
      if (h[idx] == hh && streq(k[idx].p, k[idx].n, p, n)) return v[idx];
      idx = (idx + 1) & mask;
    }
    return -1;
  }
};

// ---------------------------------------------------------------- concurrent ID interning
struct IdTable {
  u64 cap = 0, mask = 0, limit = 0;
  std::atomic<u64>* h = nullptr;
  const char** ptr = nullptr;
  u32* len = nullptr;
  std::atomic<u64> count{0};
  std::atomic<bool> overflow{false};

  static void* zalloc(size_t bytes) {
    void* m = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (m == MAP_FAILED) die("mmap anon");
#ifdef MADV_HUGEPAGE
    madvise(m, bytes, MADV_HUGEPAGE);
#endif
    return m;
  }
  void init(u64 c) {
    cap = c;
    mask = c - 1;
    limit = c * 7 / 10;
    h = (std::atomic<u64>*)zalloc(cap * 8);
    ptr = (const char**)zalloc(cap * 8);
    len = (u32*)zalloc(cap * 4);
    count = 0;
    overflow = false;
  }
  void release() {
    munmap((void*)h, cap * 8);
    munmap((void*)ptr, cap * 8);
    munmap((void*)len, cap * 4);
  }
  inline void prefetch(u64 hh) const { __builtin_prefetch((const void*)&h[hh & mask], 1, 1); }
  // local: per-thread pending insert counter
  inline u32 intern(const char* p, u32 n, u32& local) { return intern_h(p, n, hbytes(p, n), local); }
  inline u32 intern_h(const char* p, u32 n, u64 hh, u32& local) {
    u64 idx = hh & mask;
    for (;;) {
      u64 cur = h[idx].load(std::memory_order_relaxed);
      if (cur == hh) return (u32)idx;
      if (cur == 0) {
        u64 exp = 0;
        if (h[idx].compare_exchange_strong(exp, hh, std::memory_order_relaxed)) {
          ptr[idx] = p;
          len[idx] = n;
          if (++local >= 1024) {
            if (count.fetch_add(local, std::memory_order_relaxed) + local > limit) overflow.store(true);
            local = 0;
          }
          return (u32)idx;
        }
        if (exp == hh) return (u32)idx;
      }
      idx = (idx + 1) & mask;
    }
  }
};

// ---------------------------------------------------------------- arenas + block lists
// Per-thread bump allocator over a large reserved mapping (huge-page advised), so record
// storage is touched densely and sequentially. Falls back to malloc if it runs out.
struct Arena {
  char* base = nullptr;
  size_t used = 0, cap = 0;
  void init(size_t bytes) {
    cap = (bytes + 4095) & ~(size_t)4095;
    void* m = mmap(nullptr, cap, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (m == MAP_FAILED) die("mmap arena");
#ifdef MADV_HUGEPAGE
    madvise(m, cap, MADV_HUGEPAGE);
#endif
    base = (char*)m;
    used = 0;
  }
  inline void* alloc(size_t n) {
    n = (n + 63) & ~(size_t)63;
    if (used + n > cap) return malloc(n);
    void* r = base + used;
    used += n;
    return r;
  }
};
template <class T, int BLK>
struct BList {
  struct Block {
    Block* next;
    u32 n;
    T items[BLK];
  };
  Block* head = nullptr;
  Block* tail = nullptr;
  size_t total = 0;
  inline void push(const T& v, Arena& a) {
    if (!tail || tail->n == BLK) {
      Block* b = (Block*)a.alloc(sizeof(Block));
      b->next = nullptr;
      b->n = 0;
      if (tail)
        tail->next = b;
      else
        head = b;
      tail = b;
    }
    tail->items[tail->n++] = v;
    total++;
  }
  size_t size() const { return total; }
  template <class F>
  inline void each(F&& f) const {
    for (const Block* b = head; b; b = b->next)
      for (u32 i = 0; i < b->n; i++) f(b->items[i]);
  }
};

// ---------------------------------------------------------------- records
// pc: 0 = invalid event, 1 = valid view/cart, (price << 8 | category) for a valid purchase (price >= 1)
struct EvRec {  // 32 bytes
  u64 h;        // event_id hash
  i64 ingest;
  u32 h2;       // independent second event_id hash (identity check)
  u32 ts_rel;   // ts - window_start (valid only)
  u32 person;
  u32 pc;
};
struct SEv {  // 12 bytes
  u32 ts_rel;
  u32 pc;
  u32 person;
};
// Two independent hashes of a short string in one pass.
static inline u64 hbytes2(const char* p, u32 n, u32& h2) {
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
struct ClRec {
  i64 ts;
  u32 person;
  u32 camp;
  const char* cid;
  u32 cidn;
};

static inline u32 phash(u32 person) { return (u32)(fmix((u64)person + 0x1234567ULL) >> 40); }

// ---------------------------------------------------------------- Parquet writer
struct TW {
  vector<u8>& o;
  vector<int> st;
  int last = 0;
  explicit TW(vector<u8>& out) : o(out) {}
  void varint(u64 v) {
    while (v >= 0x80) {
      o.push_back((u8)(v | 0x80));
      v >>= 7;
    }
    o.push_back((u8)v);
  }
  static u64 zz(i64 v) { return ((u64)v << 1) ^ (u64)(v >> 63); }
  void fh(int id, int type) {
    int d = id - last;
    if (d > 0 && d <= 15)
      o.push_back((u8)((d << 4) | type));
    else {
      o.push_back((u8)type);
      varint(zz(id));
    }
    last = id;
  }
  void i32(int id, i64 v) {
    fh(id, 5);
    varint(zz(v));
  }
  void i64f(int id, i64 v) {
    fh(id, 6);
    varint(zz(v));
  }
  void str(int id, const string& s) {
    fh(id, 8);
    varint(s.size());
    o.insert(o.end(), s.begin(), s.end());
  }
  void sbeg(int id) {
    fh(id, 12);
    st.push_back(last);
    last = 0;
  }
  void lsbeg() {  // struct as list element
    st.push_back(last);
    last = 0;
  }
  void send() {
    o.push_back(0);
    last = st.back();
    st.pop_back();
  }
  void lhead(int id, int et, int n) {
    fh(id, 9);
    if (n < 15)
      o.push_back((u8)((n << 4) | et));
    else {
      o.push_back((u8)(0xF0 | et));
      varint(n);
    }
  }
  void lstr(const string& s) {
    varint(s.size());
    o.insert(o.end(), s.begin(), s.end());
  }
  void li32(i64 v) { varint(zz(v)); }
};

enum PType { P_INT32 = 1, P_INT64 = 2, P_BYTE_ARRAY = 6 };
struct PCol {
  string name;
  int type;
  bool optional;
  int kind;  // 0 plain, 1 string, 2 date
  u8* data = nullptr;  // PLAIN-encoded non-null values
  size_t dn = 0, dcap = 0;
  vector<u8> defs;     // bit-packed definition levels (optional only)
  i64 nvals = 0;       // rows
  inline u8* need(size_t k) {
    if (dn + k > dcap) {
      dcap = std::max(dcap * 2, dn + k + 65536);
      data = (u8*)realloc(data, dcap);
    }
    u8* r = data + dn;
    dn += k;
    return r;
  }
  void reserve(size_t bytes, size_t rows) {
    need(bytes);
    dn = 0;
    if (optional) defs.reserve(rows / 8 + 1);
  }
  inline void put_i64(i64 v) {
    memcpy(need(8), &v, 8);
    nvals++;
  }
  inline void put_i32(int32_t v) {
    memcpy(need(4), &v, 4);
    nvals++;
  }
  inline void put_str(const char* p, u32 n) {
    u8* o = need(4 + n);
    memcpy(o, &n, 4);
    memcpy(o + 4, p, n);
    if (optional) setdef(true);
    nvals++;
  }
  inline void put_null() {
    setdef(false);
    nvals++;
  }
  inline void setdef(bool present) {
    size_t i = (size_t)nvals;
    if (defs.size() * 8 <= i) defs.push_back(0);
    if (present) defs[i >> 3] |= (u8)(1u << (i & 7));
  }
};

static void write_all(int fd, const void* p, size_t n) {
  const char* c = (const char*)p;
  while (n) {
    ssize_t r = write(fd, c, n);
    if (r <= 0) die("write");
    c += r;
    n -= (size_t)r;
  }
}

static void write_parquet(const string& path, vector<PCol>& cols, i64 nrows) {
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) die(path.c_str());
  const char* magic = "PAR1";
  write_all(fd, magic, 4);
  i64 fpos = 4;
  struct Meta {
    i64 off, size;
  };
  vector<Meta> metas;
  if (nrows > 0) {
    for (auto& c : cols) {
      vector<u8> pre;  // page header + (for optional) def-level block
      vector<u8> lev;
      if (c.optional) {
        TW t(lev);
        u64 groups = (c.nvals + 7) / 8;
        t.varint((groups << 1) | 1);
        c.defs.resize(groups, 0);
        lev.insert(lev.end(), c.defs.begin(), c.defs.end());
        u32 L = (u32)lev.size();
        lev.insert(lev.begin(), (u8*)&L, (u8*)&L + 4);
      }
      i64 psize = (i64)(lev.size() + c.dn);
      TW t(pre);
      t.i32(1, 0);  // DATA_PAGE
      t.i32(2, psize);
      t.i32(3, psize);
      t.sbeg(5);
      t.i32(1, c.nvals);
      t.i32(2, 0);  // PLAIN
      t.i32(3, 3);  // RLE
      t.i32(4, 3);  // RLE
      t.send();
      pre.push_back(0);
      i64 off = fpos;
      pre.insert(pre.end(), lev.begin(), lev.end());
      write_all(fd, pre.data(), pre.size());
      if (c.dn) write_all(fd, c.data, c.dn);
      i64 total = (i64)(pre.size() + c.dn);
      fpos += total;
      metas.push_back({off, total});
    }
  }
  vector<u8> out;
  vector<u8>& fm = out;
  TW t(fm);
  t.i32(1, 1);
  t.lhead(2, 12, (int)cols.size() + 1);
  t.lsbeg();
  t.str(4, "schema");
  t.i32(5, (i64)cols.size());
  t.send();
  for (auto& c : cols) {
    t.lsbeg();
    t.i32(1, c.type);
    t.i32(3, c.optional ? 1 : 0);
    t.str(4, c.name);
    if (c.kind == 1) t.i32(6, 0);  // UTF8
    if (c.kind == 2) t.i32(6, 6);  // DATE
    if (c.kind) {
      t.sbeg(10);
      t.sbeg(c.kind == 1 ? 1 : 6);
      t.send();
      t.send();
    }
    t.send();
  }
  t.i64f(3, nrows);
  if (nrows > 0) {
    t.lhead(4, 12, 1);
    t.lsbeg();
    t.lhead(1, 12, (int)cols.size());
    i64 total = 0;
    for (size_t i = 0; i < cols.size(); i++) {
      auto& c = cols[i];
      total += metas[i].size;
      t.lsbeg();
      t.i64f(2, metas[i].off);
      t.sbeg(3);
      t.i32(1, c.type);
      t.lhead(2, 5, 2);
      t.li32(0);
      t.li32(3);
      t.lhead(3, 8, 1);
      t.lstr(c.name);
      t.i32(4, 0);
      t.i64f(5, c.nvals);
      t.i64f(6, metas[i].size);
      t.i64f(7, metas[i].size);
      t.i64f(9, metas[i].off);
      t.send();
      t.send();
    }
    t.i64f(2, total);
    t.i64f(3, nrows);
    t.send();
  } else {
    t.lhead(4, 12, 0);
  }
  t.str(6, "shopping-sessions-cpp");
  fm.push_back(0);
  u32 L = (u32)fm.size();
  size_t o = out.size();
  out.resize(o + 4);
  memcpy(&out[o], &L, 4);
  out.insert(out.end(), magic, magic + 4);
  write_all(fd, out.data(), out.size());
  close(fd);
}

// ---------------------------------------------------------------- main
struct UserRow {
  const char* name;
  u32 nlen;
  int32_t day;
  i64 sessions, events, revenue;
  int topcat;  // -1 null
};

int main() {
  const char* env;
  string DATA = (env = getenv("DATA_DIR")) ? env : "/data";
  string OUT = (env = getenv("OUTPUT_DIR")) ? env : "/output";
  int ncpu = (env = getenv("NCPU")) ? atoi(env) : (int)std::thread::hardware_concurrency();
  if (ncpu < 1) ncpu = 1;
  NT = ncpu;

  // ---- manifest
  string man = read_all(DATA + "/manifest.json");
  auto jget = [&](const char* key) -> i64 {
    size_t p = man.find(key);
    if (p == string::npos) die("manifest key");
    p = man.find(':', p);
    size_t q1 = man.find('"', p);
    size_t q2 = man.find('"', q1 + 1);
    i64 v;
    if (!parse_ts(man.data() + q1 + 1, (u32)(q2 - q1 - 1), v)) die("manifest ts");
    return v;
  };
  const i64 WS = jget("\"window_start\"");
  const i64 WE = jget("\"window_end\"");
  const i64 CWS = WS - WEEKMS;
  auto floordiv = [](i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); };
  const i64 DAY0 = floordiv(WS, DAYMS);
  const int NDAYS = (int)(floordiv(WE - 1, DAYMS) - DAY0 + 1);

  // ---- products / categories
  string prod_s = read_all(DATA + "/products.csv");
  vector<string> cats;
  vector<Str> pkeys;
  vector<int> pcat;
  {
    LookupTable catT;
    vector<Str> catkeys;
    const char* p = prod_s.data();
    const char* e = p + prod_s.size();
    bool first = true;
    while (p < e) {
      const char* nl = (const char*)memchr(p, '\n', e - p);
      const char* le = nl ? nl : e;
      if (!first) {
        Str f[2];
        if (split<2>(p, le, f) && f[0].n) {
          string c(f[1].p, f[1].n);
          int ci = -1;
          for (size_t i = 0; i < cats.size(); i++)
            if (cats[i] == c) {
              ci = (int)i;
              break;
            }
          if (ci < 0) {
            ci = (int)cats.size();
            cats.push_back(c);
          }
          pkeys.push_back(f[0]);
          pcat.push_back(ci);
        }
      }
      first = false;
      if (!nl) break;
      p = nl + 1;
    }
  }
  int UNKNOWN = -1;
  for (size_t i = 0; i < cats.size(); i++)
    if (cats[i] == "unknown") UNKNOWN = (int)i;
  if (UNKNOWN < 0) {
    UNKNOWN = (int)cats.size();
    cats.push_back("unknown");
  }
  const int NCAT = (int)cats.size();
  // Packing limits: event ts is stored relative to window_start in 32 bits, category in 8 bits.
  if (WE - WS <= 0 || WE - WS >= (1LL << 32) || NCAT > 256) {
    errno = 0;
    die("window longer than 49 days or more than 256 categories: unsupported packing");
  }
  LookupTable prodT;
  prodT.build(pkeys);
  // map table values (product index) -> category index
  for (auto& v : prodT.v)
    if (v >= 0) v = pcat[v];
  vector<int> catrank(NCAT);
  {
    vector<int> ord(NCAT);
    for (int i = 0; i < NCAT; i++) ord[i] = i;
    std::sort(ord.begin(), ord.end(), [&](int a, int b) { return cats[a] < cats[b]; });
    for (int i = 0; i < NCAT; i++) catrank[ord[i]] = i;
  }

  // ---- campaigns
  string camp_s = read_all(DATA + "/campaigns.csv");
  vector<Str> ckeys;
  vector<string> cchan;
  {
    const char* p = camp_s.data();
    const char* e = p + camp_s.size();
    bool first = true;
    while (p < e) {
      const char* nl = (const char*)memchr(p, '\n', e - p);
      const char* le = nl ? nl : e;
      if (!first) {
        Str f[2];
        if (split<2>(p, le, f) && f[0].n) {
          ckeys.push_back(f[0]);
          cchan.push_back(string(f[1].p, f[1].n));
        }
      }
      first = false;
      if (!nl) break;
      p = nl + 1;
    }
  }
  LookupTable campT;
  campT.build(ckeys);
  const int NCAMP = (int)ckeys.size();
  const int ORGANIC = NCAMP;

  // ---- files
  vector<string> evf = list_csv(DATA + "/events"), clf = list_csv(DATA + "/clicks"), lkf = list_csv(DATA + "/links");
  vector<File> evm, clm, lkm;
  size_t evbytes = 0, clbytes = 0, lkbytes = 0;
  for (auto& s : evf) evm.push_back(map_file(s)), evbytes += evm.back().size;
  for (auto& s : clf) clm.push_back(map_file(s)), clbytes += clm.back().size;
  for (auto& s : lkf) lkm.push_back(map_file(s)), lkbytes += lkm.back().size;
  size_t total = evbytes + clbytes + lkbytes;
  size_t target = std::max<size_t>(256 << 10, std::min<size_t>(8 << 20, total / ((size_t)NT * 24 + 1)));
  vector<Chunk> lchunks, mchunks;
  size_t ltarget = std::max<size_t>(32 << 10, lkbytes / ((size_t)NT * 8 + 1));
  for (auto& f : lkm) make_chunks(f, 0, ltarget, lchunks);
  for (auto& f : evm) make_chunks(f, 1, target, mchunks);
  for (auto& f : clm) make_chunks(f, 2, target, mchunks);
  // biggest chunks first for better balance
  std::sort(mchunks.begin(), mchunks.end(), [](const Chunk& a, const Chunk& b) { return (a.e - a.b) > (b.e - b.b); });
  logt("setup");

  // ---- partitions
  const size_t est_events = evbytes / 64 + 1024;
  int PE_BITS = 8;
  while (PE_BITS < 12 && (est_events >> PE_BITS) > 120000) PE_BITS++;
  const int PE = 1 << PE_BITS;
  const int PU = 256;

  // ID table sizing; retried with doubled capacity on overflow.
  // (generator: ~1 distinct ID per 13 events, ~80 bytes per event line, ~20 bytes per link line)
  u64 est_ids = evbytes / 1000 + lkbytes / 64 + 4096;
  u64 cap = 1 << 16;
  while (cap < est_ids * 4 / 3) cap <<= 1;

  IdTable ids;
  u32* parent = nullptr;  // stored as root+1, 0 = self
  u32* nameof = nullptr;  // name slot + 1 for linked roots, 0 = self
  typedef BList<EvRec, 500> EvList;
  typedef BList<ClRec, 500> ClList;
  typedef BList<SEv, 1300> SList;
  vector<vector<EvList>> evp;
  vector<vector<ClList>> clp;
  vector<Arena> arenas(NT);

  for (int attempt = 0;; attempt++) {
    ids.init(cap);
    logt("ids init");
    // ---- links
    vector<vector<std::pair<u32, u32>>> lpairs(NT);
    parallel((int)lchunks.size(), [&](int ci, int t) {
      if (ids.overflow.load(std::memory_order_relaxed)) return;
      const Chunk& c = lchunks[ci];
      u32 local = 0;
      auto& out = lpairs[t];
      scan_lines<2>(c.b, c.e, [&](Str* f) {
        if (f[0].n && f[1].n) {
          u32 a = ids.intern(f[0].p, f[0].n, local);
          u32 b = ids.intern(f[1].p, f[1].n, local);
          if (a != b) out.push_back({a, b});
        }
      });
      ids.count.fetch_add(local);
    });
    if (ids.overflow) {
      ids.release();
      cap <<= 1;
      continue;
    }
    logt("links parsed");
    // ---- union-find
    parent = (u32*)IdTable::zalloc(cap * 4);
    nameof = (u32*)IdTable::zalloc(cap * 4);
    // Lock-free union-find: a root is the only node whose parent is 0, and it changes only by
    // CAS from 0, linking the larger slot under the smaller. Path halving uses CAS too.
    auto* ap = reinterpret_cast<std::atomic<u32>*>(parent);
    auto* an = reinterpret_cast<std::atomic<u32>*>(nameof);
    auto afind = [&](u32 x) {
      for (;;) {
        u32 p = ap[x].load(std::memory_order_acquire);
        if (!p) return x;
        u32 g = ap[p - 1].load(std::memory_order_acquire);
        if (g) ap[x].compare_exchange_weak(p, g, std::memory_order_acq_rel, std::memory_order_relaxed);
        x = p - 1;
      }
    };
    parallel(NT, [&](int i, int) {
      for (auto& pr : lpairs[i]) {
        u32 a = pr.first, b = pr.second;
        for (;;) {
          a = afind(a);
          b = afind(b);
          if (a == b) break;
          if (a < b) std::swap(a, b);
          u32 z = 0;
          if (ap[a].compare_exchange_strong(z, b + 1, std::memory_order_acq_rel, std::memory_order_acquire)) break;
        }
      }
    });
    // names (byte-wise smallest ID per linked person) and flattening, now that no union is pending
    parallel(NT, [&](int i, int) {
      for (auto& pr : lpairs[i])
        for (u32 s : {pr.first, pr.second}) {
          u32 r = afind(s);
          if (r != s) ap[s].store(r + 1, std::memory_order_relaxed);
          u32 cur = an[r].load(std::memory_order_relaxed);
          for (;;) {
            u32 cs = cur ? cur - 1 : r;
            if (cs == s || strcmpb(ids.ptr[s], ids.len[s], ids.ptr[cs], ids.len[cs]) >= 0) break;
            if (an[r].compare_exchange_weak(cur, s + 1, std::memory_order_relaxed)) break;
          }
        }
    });
    logt("union-find");

    // ---- events + clicks
    evp.assign(NT, vector<EvList>(PE));
    clp.assign(NT, vector<ClList>(PU));
    // virtual reservation: any one thread could end up parsing everything
    for (auto& a : arenas) a.init(evbytes + clbytes + (64 << 20));
    const u32* par = parent;
    parallel((int)mchunks.size(), [&](int ci, int t) {
      if (ids.overflow.load(std::memory_order_relaxed)) return;
      const Chunk& c = mchunks[ci];
      u32 local = 0;
      if (c.kind == 1) {
        auto& out = evp[t];
        Arena& ar = arenas[t];
        struct PendEv {
          EvRec r;
          const char* up;
          u32 un;
          u64 uh;
        };
        constexpr int PB = 16;
        PendEv pend[PB];
        int npend = 0;
        auto flush_ev = [&]() {
          for (int i = 0; i < npend; i++) {
            PendEv& pe = pend[i];
            u32 s = ids.intern_h(pe.up, pe.un, pe.uh, local);
            pe.r.person = par[s] ? par[s] - 1 : s;  // slots inserted now are never linked
            out[pe.r.h >> (64 - PE_BITS)].push(pe.r, ar);
          }
          npend = 0;
        };
        scan_lines<7>(c.b, c.e, [&](Str* f) {
          if (!f[0].n) return;
          EvRec r;
          r.h = hbytes2(f[0].p, f[0].n, r.h2);
          if (!parse_ts(f[3].p, f[3].n, r.ingest)) r.ingest = INT64_MAX;
          r.pc = 0;
          r.person = 0;
          r.ts_rel = 0;
          i64 ts;
          if (f[1].n && f[5].n && parse_ts(f[2].p, f[2].n, ts) && ts >= WS && ts < WE && r.ingest != INT64_MAX &&
              r.ingest - ts <= DAYMS) {
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
              // defer the ID lookup: hash now, prefetch the slot, resolve in a batch
              PendEv& pe = pend[npend++];
              pe.r = r;
              pe.up = f[1].p;
              pe.un = f[1].n;
              pe.uh = hbytes(f[1].p, f[1].n);
              ids.prefetch(pe.uh);
              if (npend == PB) flush_ev();
              return;
            }
          }
          out[r.h >> (64 - PE_BITS)].push(r, ar);
        });
        flush_ev();
      } else {
        auto& out = clp[t];
        Arena& ar = arenas[t];
        struct PendCl {
          ClRec r;
          const char* up;
          u32 un;
          u64 uh;
        };
        constexpr int PB = 16;
        PendCl pend[PB];
        int npend = 0;
        auto flush_cl = [&]() {
          for (int i = 0; i < npend; i++) {
            PendCl& pc = pend[i];
            u32 s = ids.intern_h(pc.up, pc.un, pc.uh, local);
            pc.r.person = par[s] ? par[s] - 1 : s;
            out[phash(pc.r.person) & (PU - 1)].push(pc.r, ar);
          }
          npend = 0;
        };
        scan_lines<4>(c.b, c.e, [&](Str* f) {
          i64 ts;
          if (f[0].n && f[1].n && parse_ts(f[3].p, f[3].n, ts) && ts >= CWS && ts < WE) {
            int camp = campT.find(f[2].p, f[2].n);
            if (camp >= 0) {
              PendCl& pc = pend[npend++];
              pc.r.ts = ts;
              pc.r.camp = (u32)camp;
              pc.r.cid = f[0].p;
              pc.r.cidn = f[0].n;
              pc.up = f[1].p;
              pc.un = f[1].n;
              pc.uh = hbytes(f[1].p, f[1].n);
              ids.prefetch(pc.uh);
              if (npend == PB) flush_cl();
            }
          }
        });
        flush_cl();
      }
      ids.count.fetch_add(local);
    });
    if (ids.overflow) {
      ids.release();
      cap <<= 1;
      continue;
    }
    break;
  }
  logt("events+clicks parsed");
  fprintf(stderr, "ids=%llu cap=%llu\n", (unsigned long long)ids.count.load(), (unsigned long long)cap);

  // ---- dedup events per eid partition, scatter survivors by person partition
  vector<vector<SList>> sp(NT, vector<SList>(PU));
  vector<Arena> sarenas(NT);
  for (auto& a : sarenas) a.init(evbytes / 4 + (16 << 20));
  {
    vector<vector<const EvRec*>> tabs(NT);
    parallel(PE, [&](int p, int t) {
      size_t n = 0;
      for (int k = 0; k < NT; k++) n += evp[k][p].size();
      size_t sz = 16;
      while (sz < n * 2) sz <<= 1;
      auto& tab = tabs[t];
      tab.assign(sz, nullptr);
      u64 m = sz - 1;
      for (int k = 0; k < NT; k++)
        evp[k][p].each([&](const EvRec& r) {
          u64 idx = r.h & m;
          for (;;) {
            const EvRec* q = tab[idx];
            if (!q) {
              tab[idx] = &r;
              break;
            }
            if (q->h == r.h && q->h2 == r.h2) {
              if (r.ingest < q->ingest) tab[idx] = &r;
              break;
            }
            idx = (idx + 1) & m;
          }
        });
      auto& out = sp[t];
      Arena& ar = sarenas[t];
      for (const EvRec* q : tab)
        if (q && q->pc) out[phash(q->person) & (PU - 1)].push(SEv{q->ts_rel, q->pc, q->person}, ar);
    });
  }
  logt("dedup");

  // ---- sessionize + attribute per person partition
  const size_t CSZ = (size_t)(NCAMP + 1) * NDAYS;
  vector<vector<i64>> cstat(NT, vector<i64>(CSZ * 4, 0));  // sessions, purchasing, revenue, assisted
  vector<vector<UserRow>> urows(NT);
  {
    struct TL {
      vector<u64> tkey;  // local person table: (person+1) << 32 | lid
      vector<u32> persons, off, fill, lid;
      vector<u64> ev;     // ts_rel << 32 | pc, grouped by person
      vector<ClRec> cl;   // grouped by person
      vector<u32> clid;
      vector<u32> coff;
      vector<int> cnt, pos, active;
      vector<i64> catsum;
      vector<int> cattouched;
    };
    vector<TL> tls(NT);
    for (auto& tl : tls) {
      tl.cnt.assign(NCAMP + 1, 0);
      tl.pos.assign(NCAMP + 1, 0);
      tl.catsum.assign(NCAT, 0);
    }
    parallel(PU, [&](int q, int t) {
      TL& tl = tls[t];
      size_t ne = 0, ncl = 0;
      for (int k = 0; k < NT; k++) ne += sp[k][q].size(), ncl += clp[k][q].size();
      if (!ne) return;
      size_t tsz = 64;
      while (tsz < ne * 2) tsz <<= 1;
      const u64 tm = tsz - 1;
      auto& tkey = tl.tkey;
      tkey.assign(tsz, 0);
      auto& persons = tl.persons;
      persons.clear();
      auto& off = tl.off;
      off.clear();
      auto& lid = tl.lid;
      lid.resize(ne);
      // 1. local ids + counts
      size_t ix = 0;
      for (int k = 0; k < NT; k++)
        sp[k][q].each([&](const SEv& e) {
          u64 key = (u64)(e.person + 1) << 32;
          u64 idx = ((u64)e.person * 0x9E3779B97F4A7C15ULL) >> 20 & tm;
          for (;;) {
            u64 cur = tkey[idx];
            if ((cur & 0xFFFFFFFF00000000ULL) == key) {
              u32 l = (u32)cur;
              lid[ix++] = l;
              off[l]++;
              break;
            }
            if (!cur) {
              u32 l = (u32)persons.size();
              tkey[idx] = key | l;
              persons.push_back(e.person);
              off.push_back(1);
              lid[ix++] = l;
              break;
            }
            idx = (idx + 1) & tm;
          }
        });
      const size_t np = persons.size();
      // prefix sums
      u32 acc = 0;
      for (size_t l = 0; l < np; l++) {
        u32 c = off[l];
        off[l] = acc;
        acc += c;
      }
      off.push_back(acc);
      auto& fill = tl.fill;
      fill.assign(off.begin(), off.end() - 1);
      auto& ev = tl.ev;
      ev.resize(ne);
      ix = 0;
      for (int k = 0; k < NT; k++)
        sp[k][q].each([&](const SEv& e) { ev[fill[lid[ix++]]++] = ((u64)e.ts_rel << 32) | e.pc; });
      // 2. clicks grouped by the same local ids (clicks of people without events are irrelevant)
      auto& clid = tl.clid;
      clid.resize(ncl);
      auto& coff = tl.coff;
      coff.assign(np + 1, 0);
      ix = 0;
      for (int k = 0; k < NT; k++)
        clp[k][q].each([&](const ClRec& c) {
          u64 key = (u64)(c.person + 1) << 32;
          u64 idx = ((u64)c.person * 0x9E3779B97F4A7C15ULL) >> 20 & tm;
          u32 l = 0xFFFFFFFFu;
          for (;;) {
            u64 cur = tkey[idx];
            if (!cur) break;
            if ((cur & 0xFFFFFFFF00000000ULL) == key) {
              l = (u32)cur;
              break;
            }
            idx = (idx + 1) & tm;
          }
          clid[ix++] = l;
          if (l != 0xFFFFFFFFu) coff[l + 1]++;
        });
      for (size_t l = 0; l < np; l++) coff[l + 1] += coff[l];
      auto& cl = tl.cl;
      cl.resize(coff[np]);
      fill.assign(coff.begin(), coff.end() - 1);
      ix = 0;
      for (int k = 0; k < NT; k++)
        clp[k][q].each([&](const ClRec& c) {
          u32 l = clid[ix++];
          if (l != 0xFFFFFFFFu) cl[fill[l]++] = c;
        });

      i64* cs = cstat[t].data();
      auto& rows = urows[t];
      auto& cnt = tl.cnt;
      auto& pos = tl.pos;
      auto& active = tl.active;
      auto& catsum = tl.catsum;
      auto& ctouch = tl.cattouched;
      auto clk_less = [](const ClRec& a, const ClRec& b) {
        if (a.ts != b.ts) return a.ts < b.ts;
        return strcmpb(a.cid, a.cidn, b.cid, b.cidn) < 0;
      };
      for (size_t L = 0; L < np; L++) {
        const u32 P = persons[L];
        u64* e0 = ev.data() + off[L];
        u64* e1 = ev.data() + off[L + 1];
        if (e1 - e0 <= 24) {  // insertion sort for small groups
          for (u64* a = e0 + 1; a < e1; a++) {
            u64 v = *a;
            u64* b = a;
            while (b > e0 && *(b - 1) > v) {
              *b = *(b - 1);
              b--;
            }
            *b = v;
          }
        } else {
          std::sort(e0, e1);
        }
        const size_t c0 = coff[L], c1 = coff[L + 1];
        if (c1 - c0 > 1) std::sort(cl.begin() + c0, cl.begin() + c1, clk_less);
        size_t lo = c0, hi = c0;
        i64 cur_day = INT64_MIN, dsess = 0, devents = 0, drev = 0;
        u32 nslot = nameof[P] ? nameof[P] - 1 : P;
        auto flush = [&]() {
          if (cur_day == INT64_MIN) return;
          int best = -1;
          i64 bestv = 0;
          for (int ci : ctouch) {
            i64 v = catsum[ci];
            if (best < 0 || v > bestv || (v == bestv && catrank[ci] < catrank[best])) {
              best = ci;
              bestv = v;
            }
            catsum[ci] = 0;
          }
          ctouch.clear();
          rows.push_back(UserRow{ids.ptr[nslot], ids.len[nslot], (int32_t)cur_day, dsess, devents, drev, best});
          dsess = devents = drev = 0;
        };
        const u64* k = e0;
        while (k < e1) {
          const u32 srel = (u32)(*k >> 32);
          const i64 s = WS + srel;
          u32 last = srel;
          i64 nev = 0, rev = 0, npur = 0;
          i64 day = floordiv(s, DAYMS);
          if (day != cur_day) {
            flush();
            cur_day = day;
          }
          const u64* j = k;
          while (j < e1) {
            u32 tr = (u32)(*j >> 32);
            if ((u64)(tr - last) > (u64)GAPMS) break;
            last = tr;
            nev++;
            u32 pc = (u32)*j;
            if (pc > 1) {
              u32 price = pc >> 8, ct = pc & 0xFF;
              npur++;
              rev += price;
              if (catsum[ct] == 0) ctouch.push_back((int)ct);
              catsum[ct] += price;
            }
            j++;
          }
          // click window [s - 7d, s]
          while (hi < c1 && cl[hi].ts <= s) {
            int cp = (int)cl[hi].camp;
            if (cnt[cp]++ == 0) {
              pos[cp] = (int)active.size();
              active.push_back(cp);
            }
            hi++;
          }
          while (lo < hi && cl[lo].ts < s - WEEKMS) {
            int cp = (int)cl[lo].camp;
            if (--cnt[cp] == 0) {
              int last_c = active.back();
              active[pos[cp]] = last_c;
              pos[last_c] = pos[cp];
              active.pop_back();
            }
            lo++;
          }
          int credited = ORGANIC;
          if (hi > lo) {
            size_t m = hi - 1;
            i64 mts = cl[m].ts;
            while (m > lo && cl[m - 1].ts == mts) m--;
            credited = (int)cl[m].camp;
          }
          size_t di = (size_t)(day - DAY0);
          i64* r = cs + ((size_t)credited * NDAYS + di) * 4;
          r[0] += 1;
          r[1] += npur > 0;
          r[2] += rev;
          for (int a : active) cs[((size_t)a * NDAYS + di) * 4 + 3] += 1;
          dsess++;
          devents += nev;
          drev += rev;
          k = j;
        }
        flush();
        while (lo < hi) {
          int cp = (int)cl[lo].camp;
          if (--cnt[cp] == 0) {
            int last_c = active.back();
            active[pos[cp]] = last_c;
            pos[last_c] = pos[cp];
            active.pop_back();
          }
          lo++;
        }
      }
    });
  }
  logt("sessions");

  // ---- outputs
  string ud = OUT + "/user_daily", cd = OUT + "/campaign_daily";
  for (size_t k = 1; k <= OUT.size(); k++)
    if (k == OUT.size() || OUT[k] == '/') mkdir(OUT.substr(0, k).c_str(), 0755);
  mkdir(ud.c_str(), 0755);
  mkdir(cd.c_str(), 0755);
  auto make_ucols = []() {
    vector<PCol> cols(6);
    cols[0] = PCol{"user_id", P_BYTE_ARRAY, false, 1};
    cols[1] = PCol{"day", P_INT32, false, 2};
    cols[2] = PCol{"sessions", P_INT64, false, 0};
    cols[3] = PCol{"events", P_INT64, false, 0};
    cols[4] = PCol{"revenue_cents", P_INT64, false, 0};
    cols[5] = PCol{"top_category", P_BYTE_ARRAY, true, 1};
    return cols;
  };
  parallel(NT + 1, [&](int i, int) {
    if (i < NT) {
      auto& rows = urows[i];
      if (rows.empty() && i != 0) return;
      vector<PCol> cols = make_ucols();
      for (auto& r : rows) {
        cols[0].put_str(r.name, r.nlen);
        cols[1].put_i32(r.day);
        cols[2].put_i64(r.sessions);
        cols[3].put_i64(r.events);
        cols[4].put_i64(r.revenue);
        if (r.topcat >= 0)
          cols[5].put_str(cats[r.topcat].data(), (u32)cats[r.topcat].size());
        else
          cols[5].put_null();
      }
      write_parquet(ud + "/part-" + std::to_string(i) + ".parquet", cols, (i64)rows.size());
    } else {
      vector<i64> tot(CSZ * 4, 0);
      for (int t = 0; t < NT; t++)
        for (size_t k = 0; k < CSZ * 4; k++) tot[k] += cstat[t][k];
      vector<PCol> cols(7);
      cols[0] = PCol{"campaign_id", P_BYTE_ARRAY, false, 1};
      cols[1] = PCol{"channel", P_BYTE_ARRAY, false, 1};
      cols[2] = PCol{"day", P_INT32, false, 2};
      cols[3] = PCol{"sessions", P_INT64, false, 0};
      cols[4] = PCol{"purchasing_sessions", P_INT64, false, 0};
      cols[5] = PCol{"revenue_cents", P_INT64, false, 0};
      cols[6] = PCol{"assisted_sessions", P_INT64, false, 0};
      i64 n = 0;
      for (int cpi = 0; cpi <= NCAMP; cpi++)
        for (int d = 0; d < NDAYS; d++) {
          i64* r = &tot[((size_t)cpi * NDAYS + d) * 4];
          if (r[0] == 0 && r[3] == 0) continue;
          if (cpi == ORGANIC) {
            cols[0].put_str("organic", 7);
            cols[1].put_str("organic", 7);
          } else {
            cols[0].put_str(ckeys[cpi].p, ckeys[cpi].n);
            cols[1].put_str(cchan[cpi].data(), (u32)cchan[cpi].size());
          }
          cols[2].put_i32((int32_t)(DAY0 + d));
          cols[3].put_i64(r[0]);
          cols[4].put_i64(r[1]);
          cols[5].put_i64(r[2]);
          cols[6].put_i64(cpi == ORGANIC ? 0 : r[3]);
          n++;
        }
      write_parquet(cd + "/part-0.parquet", cols, n);
    }
  });
  logt("written");
  fflush(stderr);
  _exit(0);
}

// Fuzz scan_lines2 (two-pass) against scan_lines (one-pass) and a naive reference splitter.
#define main real_main
#include "../src/main.cpp"
#undef main
#include <random>

template <int N>
static bool naive(const char* b, const char* e, Str* out) {
  // reference implementation of the dialect for one line [b,e)
  int k = 0;
  const char* s = b;
  for (;;) {
    const char* c = s;
    while (c < e && *c != ',') c++;
    if (k == N) return false;
    u32 L = (u32)(c - s);
    int q = 0;
    for (u32 i = 0; i < L; i++) q += s[i] == '"';
    if (q) {
      if (q != 2 || L < 2 || s[0] != '"' || s[L - 1] != '"') return false;
      out[k] = Str{s + 1, L - 2};
    } else
      out[k] = Str{s, L};
    k++;
    if (c == e) break;
    s = c + 1;
  }
  return k == N;
}

template <int N>
static int check(std::mt19937_64& rng) {
  const char* alpha = "ab,,\"\"\n\n x1\r";
  size_t len = rng() % 3000;
  string buf(len, 'a');
  for (auto& ch : buf) ch = (rng() % 3) ? alpha[rng() % strlen(alpha)] : (char)('a' + rng() % 26);
  // sprinkle well-formed lines too
  for (int i = 0; i < 20; i++) {
    string line;
    for (int k = 0; k < N; k++) {
      if (k) line += ',';
      bool q = rng() % 4 == 0;
      if (q) line += '"';
      int fl = rng() % 6;
      for (int j = 0; j < fl; j++) line += (char)('a' + rng() % 26);
      if (q) line += '"';
    }
    line += '\n';
    size_t at = len ? rng() % len : 0;
    buf.insert(at, line);
  }
  buf += '\n';
  // pad so reads past the end stay in our buffer
  string padded = string(63, '\n') + string(1, (rng() % 2) ? '"' : '\n') + buf + string(64, '\0');
  const char* b = padded.data() + 64;
  const char* e = b + buf.size();
  vector<string> a, c, r;
  auto rec = [&](vector<string>& v) {
    return [&v](Str* f) {
      string s;
      for (int k = 0; k < N; k++) s += "[" + string(f[k].p, f[k].n) + "]";
      v.push_back(s);
    };
  };
  scan_lines<N>(b, e, rec(a));
  scan_lines2<N>(b, e, rec(c));
  const char* p = b;
  while (p < e) {
    const char* nl = (const char*)memchr(p, '\n', e - p);
    Str f[N];
    if (naive<N>(p, nl, f)) rec(r)(f);
    p = nl + 1;
  }
  if (a != r || c != r) {
    fprintf(stderr, "MISMATCH N=%d: one-pass %zu two-pass %zu naive %zu\n", N, a.size(), c.size(), r.size());
    return 1;
  }
  return 0;
}

int main() {
  std::mt19937_64 rng(7);
  for (int it = 0; it < 20000; it++) {
    if (check<2>(rng) || check<4>(rng) || check<7>(rng)) return 1;
  }
  printf("ok\n");
  return 0;
}

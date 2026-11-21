// A/B of the per-line event parse path: two copies of the pipeline (A = BASE_SRC, B = src/main.cpp)
// in one binary, alternated chunk by chunk so machine noise hits both equally.
// usage: benchab EVENTS.csv   (build with -DBASE_SRC='"path/to/base.cpp"')
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
#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__ARM_NEON)
#include <arm_neon.h>
#endif
#ifndef BASE_SRC
#define BASE_SRC "../src/main.cpp"
#endif

#define main real_main_a
namespace A {
#include BASE_SRC
#ifdef PIPE_TS_A
#define PIPE_TS PIPE_TS_A
#endif
#include "pipe.inc"
#undef PIPE_TS
}  // namespace A
#undef main
#define main real_main_b
namespace B {
#include "../src/main.cpp"
#ifdef PIPE_TS_B
#define PIPE_TS PIPE_TS_B
#endif
#include "pipe.inc"
#undef PIPE_TS
}  // namespace B
#undef main

static double now() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

int main(int argc, char** argv) {
  B::File f = B::map_file(argv[1]);
  const char* b = (const char*)memchr(f.data, '\n', f.size) + 1;
  const char* e = f.data + f.size;
  while (e[-1] != '\n') e--;
  std::string path = argv[1], dir = path.substr(0, path.rfind("/events/"));
  std::string prod_s = B::read_all(dir + "/products.csv"), man = B::read_all(dir + "/manifest.json");
  auto jget = [&](const char* key) -> int64_t {
    size_t q = man.find(key);
    q = man.find(':', q);
    size_t q1 = man.find('"', q), q2 = man.find('"', q1 + 1);
    int64_t v = 0;
    B::parse_ts(man.data() + q1 + 1, (uint32_t)(q2 - q1 - 1), v);
    return v;
  };
  std::vector<B::Str> pk;
  {
    const char* p = (const char*)memchr(prod_s.data(), '\n', prod_s.size()) + 1;
    B::scan_lines<2>(p, prod_s.data() + prod_s.size(), [&](B::Str* fl) { pk.push_back(fl[0]); });
  }
  std::vector<A::Str> pka;
  for (auto& s : pk) pka.push_back(A::Str{s.p, s.n});
  A::LookupTable pa;
  pa.build(pka);
  B::LookupTable pb;
  pb.build(pk);
  A::PipeCtx ca{jget("\"window_start\""), jget("\"window_end\""), &pa};
  B::PipeCtx cb{ca.WS, ca.WE, &pb};
  std::string camp_s = B::read_all(dir + "/campaigns.csv");
  std::vector<B::Str> ck;
  {
    const char* p = (const char*)memchr(camp_s.data(), '\n', camp_s.size()) + 1;
    B::scan_lines<2>(p, camp_s.data() + camp_s.size(), [&](B::Str* fl) { ck.push_back(fl[0]); });
  }
  std::vector<A::Str> cka;
  for (auto& s : ck) cka.push_back(A::Str{s.p, s.n});
  A::LookupTable cta;
  cta.build(cka);
  B::LookupTable ctb;
  ctb.build(ck);
  const int64_t CWS = ca.WS - 7 * 86400000LL;
  // the largest clicks file
  std::string cpath;
  {
    size_t best = 0;
    for (auto& s : B::list_csv(dir + "/clicks")) {
      struct stat st;
      if (stat(s.c_str(), &st) == 0 && (size_t)st.st_size > best) best = st.st_size, cpath = s;
    }
  }
  B::File cf = B::map_file(cpath);
  const char* cb0 = (const char*)memchr(cf.data, '\n', cf.size) + 1;
  const char* ce0 = cf.data + cf.size;
  while (ce0[-1] != '\n') ce0--;
  std::vector<std::pair<const char*, const char*>> cchunks;
  for (const char* p = cb0; p < ce0;) {
    const char* q = p + (256 << 10);
    q = q >= ce0 ? ce0 : (const char*)memchr(q, '\n', ce0 - q) + 1;
    cchunks.push_back({p, q});
    p = q;
  }
  size_t clines = 0;
  B::scan_lines<4>(cb0, ce0, [&](B::Str*) { clines++; });
  std::vector<std::pair<const char*, const char*>> chunks;
  for (const char* p = b; p < e;) {
    const char* q = p + (256 << 10);
    q = q >= e ? e : (const char*)memchr(q, '\n', e - q) + 1;
    chunks.push_back({p, q});
    p = q;
  }
  size_t lines = 0;
  B::scan_lines<7>(b, e, [&](B::Str*) { lines++; });
  const int ROUNDS = getenv("ROUNDS") ? atoi(getenv("ROUNDS")) : 7;
  // per chunk, the minimum over rounds (robust to preemption on a loaded machine), summed
  for (int mode = 0; mode < 3; mode++) {
    auto& chunks_ = mode == 2 ? cchunks : chunks;
    const size_t lines_ = mode == 2 ? clines : lines;
    std::vector<double> ma(chunks_.size(), 1e9), mb(chunks_.size(), 1e9);
    uint64_t xa = 0, xb = 0;
    for (int r = 0; r < ROUNDS; r++) {
      xa = xb = 0;
      for (size_t i = 0; i < chunks_.size(); i++) {
        auto [p, q] = chunks_[i];
        bool aFirst = (i + r) & 1;
        for (int k = 0; k < 2; k++) {
          double t = now();
          if ((k == 0) == aFirst) {
            xa += mode == 2 ? A::pipe_clicks(p, q, CWS, ca.WE, cta) : mode ? A::pipe_events(p, q, ca) : A::pipe_tokenize(p, q);
            ma[i] = std::min(ma[i], now() - t);
          } else {
            xb += mode == 2 ? B::pipe_clicks(p, q, CWS, cb.WE, ctb) : mode ? B::pipe_events(p, q, cb) : B::pipe_tokenize(p, q);
            mb[i] = std::min(mb[i], now() - t);
          }
        }
      }
    }
    double sa = 0, sb = 0;
    for (size_t i = 0; i < chunks_.size(); i++) sa += ma[i], sb += mb[i];
    sa *= 1e9 / lines_;
    sb *= 1e9 / lines_;
    printf("%-9s A %6.2f  B %6.2f ns/line  B-A %+5.2f (%+.1f%%)  %s\n", mode == 2 ? "clicks" : mode ? "pipeline" : "tokenize", sa, sb, sb - sa,
           100 * (sb - sa) / sa, xa == xb ? "same" : "CHECKSUM DIFFERS");
  }
}

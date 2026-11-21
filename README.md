# Shopping Sessions — native C++ pipeline

A single, dependency-free C++20 binary that implements `rules.md` for the Databricks
Shopping Sessions track, statically linked into a `FROM scratch` image (~1.3 MB).

## How it works

1. **Lookups.** `manifest.json`, `products.csv` and `campaigns.csv` are read into small hash tables.
2. **Tokenizing.** Every CSV is `mmap`ed and cut into line-aligned chunks that 8 threads pull from a queue.
   A 64-byte SIMD tokenizer (AVX2 on x86, NEON on arm64) finds newlines, commas and quotes in one pass.
   A line is well-formed when it has the exact field count and its quote count equals 2 × the number of fields
   that start and end with a quote.
3. **Linked IDs.** Link lines are interned into a lock-free hash table (CAS on 64-bit hashes). A lock-free
   union-find joins them, and each person is named by the byte-wise smallest of their IDs.
4. **Events and clicks.** Timestamps are parsed with SWAR fast paths (F1/F2 via a month table, F3 via 8-digit
   SWAR), with exact validation. User IDs are hashed, the table slot is prefetched, and lookups are resolved in
   batches of 16. Events are scattered into 32-byte records by event_id hash; valid clicks go into 32-byte records
   by person hash. Both use per-thread bump arenas with linked 16 KB blocks.
5. **Dedup.** For each event_id partition, the copy with the smallest `ingest_ts` wins. The kept copy's validity
   (computed while parsing, late rule included) decides whether it survives. Survivors become 12-byte records,
   scattered by person.
6. **Sessions.** For each person partition: a counting sort groups records by person, then each person's records
   are sorted by timestamp and split into sessions on gaps over 30 minutes. A sliding 7-day click window gives the
   credited click (latest `ts`, then the smallest `click_id`) and the set of assisting campaigns, kept as
   per-campaign counts plus an active list.
7. **Output.** A minimal hand-written Parquet writer (PLAIN, uncompressed, `day` as INT32 `DATE`) writes one
   `user_daily` file per thread and one `campaign_daily` file.

## Testing

```sh
make build && make fixtures          # 36 edge-case fixtures, in the container with scorer flags
make run-dev && make check-dev       # 1M-event dev set vs dev-answer
```

It was also checked row for row against an independent pure-Python reference implementation on generator
seeds 2, 3 and 4. The timestamp fast paths are fuzzed against a straightforward parser (`test/tsfuzz.cpp`).

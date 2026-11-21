#!/bin/bash
# Correctness gate for a native build: fixtures, dev answer, reference seeds s2-s4,
# and optionally a big20 diff against a baseline output folder.
# usage: scripts/gate.sh BINARY [BASELINE_BIG20_OUT]
set -u
cd "$(dirname "$0")/.."
BIN=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
PY=/Users/sarthak/development/buildathon/.venv/bin/python
D=/Users/sarthak/development/buildathon/data
R=/Users/sarthak/development/buildathon/reference
OUT=build/gate_$$_$(date +%s)
fail=0
$PY scripts/run_fixtures.py fixtures -- "$BIN" 2>&1 | tail -1
run() {  # name data expected
  DATA_DIR=$2 OUTPUT_DIR=$OUT/$1 "$BIN" 2>/dev/null || { echo "$1: RUN FAILED"; fail=1; return; }
  r=$(cd scripts && $PY compare.py ../$OUT/$1 "$3" 2>&1 | tail -1)
  echo "$1: $r"
  [[ "$r" == *MATCH* && "$r" != *DIFFER* ]] || fail=1
}
run dev $D/dev /Users/sarthak/development/buildathon/unz/dev-answer/dev-answer
for s in s2 s3 s4; do run $s $D/$s $R/out_$s; done
if [ $# -ge 2 ]; then run big20 $D/big20 "$(cd "$2" && pwd)"; fi
echo "outputs in $OUT"
exit $fail

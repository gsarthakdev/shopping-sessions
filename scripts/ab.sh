#!/bin/bash
# Interleaved A/B of images on the big20 volume; prints per-phase deltas per run.
# usage: scripts/ab.sh NCPU ROUNDS IMAGE...
NCPU=$1
ROUNDS=$2
shift 2
for r in $(seq 1 "$ROUNDS"); do
  for im in "$@"; do
    out=$(docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m --tmpfs /output:rw,mode=1777 \
      --cpus 8 --memory ${MEM:-12g} --user 1000:1000 -v ${VOL:-big20}:/data:ro -e NCPU="$NCPU" ${EXTRA:-} "$im" 2>&1 | tr '\n' ' ')
    echo "$im $out" | python3 -c '
import re,sys
l=sys.stdin.read(); im=l.split()[0]
t=dict((n,float(v)) for v,n in re.findall(r"\[\s*([0-9.]+)\] ([a-z+\-]+(?: [a-z+]+)?)",l))
keys=list(t); prev=0; parts=[]
for k in keys:
  parts.append("%s=%.0f"%(k.split()[0][:6],(t[k]-prev)*1000)); prev=t[k]
print(im, " ".join(parts), "TOTAL=%.0f"%(prev*1000))'
  done
done

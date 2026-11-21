#!/bin/sh
# Time the image on a dataset stored in a Docker volume (inside the Linux VM, so file
# reads and page faults behave like the scorer rather than a host bind mount).
# usage: bench_vol.sh IMAGE VOLUME [RUNS]
set -eu
IMAGE=$1
VOL=$2
RUNS=${3:-3}
i=0
while [ "$i" -lt "$RUNS" ]; do
  s=$(python3 -c 'import time;print(time.time())')
  docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --tmpfs /output:rw,mode=1777 --tmpfs /scratch:rw,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --cpus 8 --memory 6g --user 1000:1000 \
    -v "$VOL":/data:ro -e NCPU=8 -e MEM_GB=6 "$IMAGE" 2>&1 | tr '\n' ' '
  e=$(python3 -c 'import time;print(time.time())')
  python3 -c "print(' | container wall %.3fs' % ($e-$s))"
  i=$((i + 1))
done

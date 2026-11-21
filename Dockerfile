# Shopping Sessions: native C++ pipeline, statically linked into an empty image.
FROM gcc:14-bookworm AS build
WORKDIR /src
COPY src/ src/
# Scorers are Intel Xeon (Cascade Lake or newer): target AVX2 (x86-64-v3) there.
# Any other build arch (e.g. arm64 for local loops) uses the compiler default.
RUN if [ "$(uname -m)" = "x86_64" ]; then ARCH="-march=x86-64-v3 -mtune=cascadelake"; else ARCH=""; fi; \
    g++ -std=c++20 -O3 $ARCH -static -pthread -fno-plt -o /shopping-sessions src/main.cpp

FROM scratch
COPY --from=build /shopping-sessions /shopping-sessions
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
CMD ["/shopping-sessions"]

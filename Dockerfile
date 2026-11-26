# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Stage 1: build
#
# Compilers, headers, and CMake weigh several hundred megabytes and none of it
# is needed to *run* the server. This stage is discarded; only the two binaries
# it produces are carried forward.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS build

RUN apt-get update \
 && apt-get install --no-install-recommends --yes \
      build-essential \
      cmake \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy only what the build reads. Listing the inputs explicitly (rather than
# `COPY . .`) keeps the layer cache valid when documentation or scripts change.
COPY CMakeLists.txt ./
COPY include ./include
COPY src ./src
COPY tests ./tests

# Two builds. The first is ASan+UBSan and only exists to run the test suite:
# an image whose code trips a sanitizer must never reach a registry. The second
# is the optimised binary that ships. Warnings are errors in both (the CMake
# default).
RUN cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DKVSTORE_SANITIZE=ON \
 && cmake --build build-asan --parallel "$(nproc)" \
 && ctest --test-dir build-asan --output-on-failure \
 && cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build --parallel "$(nproc)" \
 && cmake --install build --prefix /opt/kvstore

# ---------------------------------------------------------------------------
# Stage 2: runtime
#
# The base image plus a C++ runtime and two static-ish binaries. No compiler,
# no source, no package manager metadata.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
 && apt-get install --no-install-recommends --yes libstdc++6 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/kvstore/bin/kvstore     /usr/local/bin/kvstore
COPY --from=build /opt/kvstore/bin/kvstore-cli /usr/local/bin/kvstore-cli

# Run as an unprivileged user. A network daemon that does not need root should
# not have it: a remote code execution bug then lands in a nobody account
# instead of on the whole container filesystem.
RUN useradd --system --create-home --shell /usr/sbin/nologin kvstore \
 && mkdir -p /data \
 && chown kvstore:kvstore /data

# The log lives on a volume so it outlives the container. Without this, every
# `docker rm` silently discards the database.
VOLUME ["/data"]
WORKDIR /data
USER kvstore

EXPOSE 6379

# The server binds 127.0.0.1 by default, which inside a container means "this
# container only" and would be unreachable from the host. 0.0.0.0 is correct
# here precisely because the container's network namespace is the boundary --
# publish the port narrowly on the host side (see docker-compose.yml).
ENTRYPOINT ["kvstore"]
CMD ["--bind", "0.0.0.0", "--port", "6379", "--wal", "/data/wal.log", "--fsync", "always"]

# Exec form so the server is PID 1 and receives SIGTERM directly from
# `docker stop`, letting it shut down through its own signal handler.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["kvstore-cli", "--host", "127.0.0.1", "--port", "6379", "PING"]

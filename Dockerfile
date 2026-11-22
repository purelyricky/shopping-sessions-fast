# Stage 1: build the Rust engine (build has network; run does not).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY fast/ /src/
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked && cp target/release/shopping-sessions /shopping-sessions

# Stage 2: minimal runtime. The scorer runs as uid 1000, read-only root fs, no network.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /shopping-sessions /app/shopping-sessions
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
WORKDIR /app
CMD ["/app/shopping-sessions"]

# shopping-sessions-fast

Fast click-stream -> Parquet pipeline for the Databricks Shopping Sessions track (Builders Buildathon 2026). MIT licensed.

- `fast/` - the Rust engine (the submitted program), built by the root `Dockerfile` (multi-stage, runtime image is debian-slim + one binary).
- `oracle/oracle.py` - slow pure-Python reference of `rules.md`, used only to cross-check the engine (not part of the image).
- `fixtures/`, `scripts/` - the 36 edge-case fixtures and checkers from the official starter kit (unchanged).
- `rules.md` - the spec (copied from the track page).

## Third-party code

Rust crates: ahash, libc, memchr, memmap2, mimalloc, rayon, radsort, serde_json, parquet (arrow-rs). Base images: `rust:1-slim-bookworm` (build), `debian:bookworm-slim` (run). Oracle/test scripts use Python, pyarrow, polars, duckdb (not shipped in the image). Fixtures, `compare.py`, `run_fixtures.py` and the data generator come from the organizers' kit.

## Approach

Memory-mapped CSV read in parallel chunks, strict hand-written CSV/timestamp parsing per the rules, exact event-id dedup before validation, union-find over interned IDs (person name = byte-wise smallest ID), per-person time sort, single-pass sessionization, sliding 7-day click window for credit and assists, direct Parquet writing.

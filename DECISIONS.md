# DECISIONS - Shopping Sessions

Curated current truth. History in log.txt.

- [claude-code 11:55] Spec source of truth: ./rules.md (copied from the track page). Fixtures: ./fixtures (36), checker: scripts/run_fixtures.py, scripts/compare.py.
- [claude-code 11:53] oracle/oracle.py = slow pure-Python reference; 36/36 fixtures, matches dev-answer exactly. Equivalence oracle for the Rust engine.
- [claude-code 11:55] Engine = Rust crate in fast/ (codex), bin name `shopping-sessions`, built by multi-stage Dockerfile (rust:1-slim -> debian:bookworm-slim). RUSTFLAGS target-cpu=x86-64-v3 (not native: build host may differ). Cargo.lock must be committed (--locked).
- Key spec traps: only '\n' ends a line ('\r' is data); quotes only around whole field; ASCII digits only; dedup events before validation (min ingest_ts); late = ingest-ts > 24h strictly; sessions split on gap > 1_800_000 ms between distinct ts; day = UTC date of session start; click window [start-7d, start], latest ts then smallest click_id bytes; assists = distinct campaigns in that window; link names = byte-wise min ID, links can name people absent from events.

#!/usr/bin/env python3
"""Run parser ablation rungs in fresh containers on prepared ssdata20.

The benchmark image must include /app/scan-floor built with Cargo feature bench.
Usage: ablation.py IMAGE [r0 r1 ... r8 r3fast r6scalar r7scalar r8scalar]
"""
import subprocess
import sys


def main():
    image = sys.argv[1]
    allowed = [f"r{i}" for i in range(9)] + ["r3fast", "r6scalar", "r7scalar", "r8scalar"]
    rungs = sys.argv[2:] or ["r0", "r1", "r2", "r3", "r3fast", "r4", "r5", "r6", "r7", "r8"]
    if any(rung not in allowed for rung in rungs):
        raise ValueError("unknown ablation rung")
    subprocess.run(["docker", "volume", "inspect", "ssdata20"], check=True, stdout=subprocess.DEVNULL)
    for rung in rungs:
        print(f"ABLATION image={image} rung={rung}", flush=True)
        subprocess.run([
            "docker", "run", "--rm", "--network", "none", "--read-only",
            "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--pids-limit", "4096",
            "--cpus", "8", "--memory", "12g", "--memory-swap", "12g", "--user", "1000:1000",
            "-v", "ssdata20:/data:ro", "-e", "NCPU=8", "-e", "PROFILE=1",
            "--entrypoint", "/app/scan-floor", image, "/data", rung], check=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

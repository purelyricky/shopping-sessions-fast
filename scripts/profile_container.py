#!/usr/bin/env python3
"""Profile an image against the track's prepared Linux-volume practice data.

Usage: profile_container.py IMAGE OUTPUT_PREFIX [REPEATS]
Requires Docker volume ssdata20 to contain a generated dataset. Outputs remain
in ssout_profile/OUTPUT_PREFIX-N; PROFILE is opt-in in the Rust engine.
"""
import datetime
import re
import subprocess
import sys


def main():
    image, prefix = sys.argv[1:3]
    repeats = int(sys.argv[3]) if len(sys.argv) > 3 else 2
    if not re.fullmatch(r"[A-Za-z0-9_-]+", prefix):
        raise ValueError("output prefix must use letters, digits, underscores or hyphens")
    subprocess.run(["docker", "volume", "inspect", "ssdata20"], check=True, stdout=subprocess.DEVNULL)
    subprocess.run([
        "docker", "run", "--rm", "--network", "none", "-v", "ssout_profile:/output",
        "-v", "ssscr_profile:/scratch", "--entrypoint", "chown", image,
        "1000:1000", "/output", "/scratch"], check=True)
    for run in range(repeats):
        print(f"image={image} output={prefix}-{run} started={datetime.datetime.now().isoformat()}", flush=True)
        subprocess.run([
            "docker", "run", "--rm", "--network", "none", "--read-only",
            "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--pids-limit", "4096",
            "--cpus", "8", "--memory", "12g", "--memory-swap", "12g", "--user", "1000:1000",
            "-v", "ssdata20:/data:ro", "-v", "ssout_profile:/output", "-v", "ssscr_profile:/scratch",
            "-e", "DATA_DIR=/data", "-e", f"OUTPUT_DIR=/output/{prefix}-{run}",
            "-e", "SCRATCH_DIR=/scratch", "-e", "HOME=/scratch", "-e", "NCPU=8", "-e", "PROFILE=1",
            image], check=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

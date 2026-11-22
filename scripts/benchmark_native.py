#!/usr/bin/env python3
"""Run a native benchmark with a wall-time cap and Windows peak-memory telemetry.

Usage: benchmark_native.py BINARY DATA_DIR OUTPUT_DIR [TIMEOUT_SECONDS]
Engine stderr is streamed unchanged. This is a local development helper only.
"""
import ctypes
import os
from pathlib import Path
import subprocess
import sys
import time


def main():
    binary, data, output = [str(Path(p).resolve()) for p in sys.argv[1:4]]
    timeout = float(sys.argv[4]) if len(sys.argv) > 4 else 180.0
    started = time.monotonic()
    process = subprocess.Popen([binary, data, output], env=dict(os.environ, NCPU="8"))
    peak_rss = peak_private = 0
    if os.name == "nt":
        size = ctypes.c_size_t

        class Counters(ctypes.Structure):
            _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong)] + [
                (name, size) for name in (
                    "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                    "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage",
                    "PagefileUsage", "PeakPagefileUsage", "PrivateUsage")]

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = ctypes.c_void_p
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        psapi = ctypes.WinDLL("psapi")
        psapi.GetProcessMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.POINTER(Counters), ctypes.c_ulong]
        handle = kernel.OpenProcess(0x410, False, process.pid)
    timed_out = False
    try:
        while process.poll() is None:
            if os.name == "nt" and handle:
                counters = Counters()
                counters.cb = ctypes.sizeof(counters)
                if psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
                    peak_rss = max(peak_rss, counters.PeakWorkingSetSize)
                    peak_private = max(peak_private, counters.PrivateUsage)
            if time.monotonic() - started > timeout:
                timed_out = True
                process.terminate()
                break
            time.sleep(0.25)
        code = process.wait()
    finally:
        if os.name == "nt" and handle:
            kernel.CloseHandle(handle)
    print(f"wall={time.monotonic()-started:.3f}s exit={code} timeout={timed_out} "
          f"peak_rss={peak_rss/2**30:.3f}GiB peak_private={peak_private/2**30:.3f}GiB", flush=True)
    return 124 if timed_out else code


if __name__ == "__main__":
    sys.exit(main())

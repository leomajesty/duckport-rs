"""Keep DuckDB inside the machine's memory while the upgrade does heavy work.

A server.env written for a bigger box (the installer's default used to be a flat 8 GB)
leaves DuckDB believing it may use more than the host has. It then never spills to disk,
and the kernel kills the server in the middle of a rebuild: grid102 has 3.8 GB, a limit
of 8 GB, and died at 2.4 GB RSS while copying usdt_perp_5m.

The machine's total is the wrong yardstick: grid102 also runs a 1 GB panel, and half of
its 3.8 GB (2 GB) is exactly where the first kill happened. What counts is what the OS can
actually hand out -- MemAvailable plus what DuckDB already holds -- and DuckDB's process
is a few hundred MB bigger than its buffer pool. So the budget is half of that sum,
never above half the machine, in 256 MiB steps, at least 512 MiB.

memory_limit is a global DuckDB setting and can be changed over Flight, so the heavy
steps run under that budget and the configured value is put back afterwards -- unless it
is itself above the host's memory, in which case the budget stays until the next restart
and the operator is told to fix server.env (`duckport upgrade` offers to do it).
"""

import re
import subprocess
from typing import Optional, Tuple

_UNIT = {"": 1, "b": 1, "kb": 1000, "kib": 1024, "mb": 1000 ** 2, "mib": 1024 ** 2,
         "gb": 1000 ** 3, "gib": 1024 ** 3, "tb": 1000 ** 4, "tib": 1024 ** 4}
STEP = 256 * 1024 ** 2
FLOOR = 512 * 1024 ** 2


def parse_size(text) -> Optional[int]:
    """'3.7 GiB', '8GB', '953.6 MiB', '1234B' -> bytes; None when unparseable."""
    m = re.match(r"\s*([\d.]+)\s*([a-zA-Z]*)\s*$", str(text or ""))
    if not m or m.group(2).lower() not in _UNIT:
        return None
    return int(float(m.group(1)) * _UNIT[m.group(2).lower()])


def fmt(n) -> str:
    return f"{n / 1024 ** 3:.1f} GiB"


def host_memory() -> Tuple[Optional[int], Optional[int]]:
    """(total, available) in bytes. Linux from /proc/meminfo, macOS from sysctl and
    vm_stat (free + inactive + speculative pages); (None, None) when unknown."""
    total = avail = None
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    total = int(line.split()[1]) * 1024
                elif line.startswith("MemAvailable:"):
                    avail = int(line.split()[1]) * 1024
        return total, avail
    except OSError:
        pass
    try:
        total = int(subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True,
                                   check=True).stdout.strip())
        vm = subprocess.run(["vm_stat"], capture_output=True, text=True, check=True).stdout
        page = int(re.search(r"page size of (\d+) bytes", vm).group(1))
        pages = {m.group(1): int(m.group(2))
                 for m in re.finditer(r"^Pages (free|inactive|speculative):\s+(\d+)", vm, re.M)}
        return total, sum(pages.values()) * page
    except (OSError, subprocess.SubprocessError, AttributeError, ValueError):
        return total, None


def suggest(total, available, in_use) -> Optional[int]:
    """A limit this host can honour right now: half of (available + what DuckDB holds),
    at most half the machine, in 256 MiB steps, at least 512 MiB. None when unknown."""
    if available is None:
        return None
    b = (available + (in_use or 0)) // 2
    if total:
        b = min(b, total // 2)
    return max(b // STEP * STEP, FLOOR)


def budget(limit, total, available, in_use) -> Optional[int]:
    """Bytes DuckDB may use during the window: the configured limit, or the suggestion
    when that is lower; None when nothing is known."""
    s = suggest(total, available, in_use)
    caps = [c for c in (limit, s) if c]
    return min(caps) if caps else None


def mib(n) -> str:
    """The form written to server.env and accepted by DuckDB."""
    return f"{n // 1024 ** 2}MiB"


def plan_note(dp) -> Optional[str]:
    """One line for the plan when the configured limit cannot be honoured on this host."""
    limit = parse_size(dp.scalar("SELECT current_setting('memory_limit')"))
    total, avail = host_memory()
    if not (limit and total and limit > total):
        return None
    s = suggest(total, avail, parse_size(dp.scalar("SELECT memory_usage FROM pragma_database_size()")))
    return (f"内存：服务端 memory_limit {fmt(limit)} 超过本机 {fmt(total)}，窗口内会临时降为 {fmt(s)}；"
            f"请把 server.env 的 DUCKPORT_DUCKDB_MEMORY_LIMIT 改为 {mib(s)} 左右（本机现在能给出的一半）并重启服务端"
            f"——`duckport upgrade` 的服务端阶段会询问并代改")


class Bounded:
    """`with Bounded(dp):` around the heavy work. Prints what it changed."""

    def __init__(self, dp, enabled=True, log=print):
        self.dp, self.enabled, self.log = dp, enabled, log
        self.applied = None

    def __enter__(self):
        if not self.enabled:
            return self
        self.original = self.dp.scalar("SELECT current_setting('memory_limit')")
        limit = parse_size(self.original)
        total, avail = host_memory()
        in_use = parse_size(self.dp.scalar("SELECT memory_usage FROM pragma_database_size()"))
        b = budget(limit, total, avail, in_use)
        if b and limit and b < limit * 0.9:
            self.dp.execute(f"SET memory_limit = '{b // 1024 ** 2}MiB'")
            self.applied, self.over = b, bool(total and limit > total)
            self.log(f"  内存：memory_limit {fmt(limit)}" + (f" 超过本机 {fmt(total)}" if self.over else "")
                     + f"，降为 {fmt(b)}" + (f"（本机可用 {fmt(avail)}）" if avail else ""))
        return self

    def __exit__(self, *exc):
        if self.applied is None:
            return False
        if self.over:
            self.log(f"  内存：memory_limit 保持 {fmt(self.applied)} 直到服务端重启；"
                     "请把 server.env 的 DUCKPORT_DUCKDB_MEMORY_LIMIT 改为不超过本机内存的值")
        else:
            self.dp.execute(f"SET memory_limit = '{parse_size(self.original) // 1024 ** 2}MiB'")
        return False

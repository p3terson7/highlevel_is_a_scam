from collections import defaultdict
from threading import Lock


_METRICS: defaultdict[str, int] = defaultdict(int)
_LOCK = Lock()


def incr(metric_name: str, amount: int = 1) -> None:
    with _LOCK:
        _METRICS[metric_name] += amount


def snapshot() -> dict[str, int]:
    with _LOCK:
        return dict(_METRICS)


def render_prometheus() -> str:
    lines: list[str] = []
    metrics = snapshot()
    for key, value in sorted(metrics.items()):
        lines.append(f"{key} {value}")
    return "\n".join(lines) + "\n"

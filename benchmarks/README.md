# Benchmarks

Run hot-path probes from the repository root:

```bash
pipx run --spec '.[dev]' python -m benchmarks.benchmark_hot_paths
```

Results are informational because absolute timings vary by machine. Compare
results on the same host when changing registry lookup or track merge logic.

"""
benchmark_report.py — CLI-Tool für Algorithmen-Benchmark.

Ruft den /benchmark Endpoint auf und:
- Gibt eine Vergleichstabelle der drei Algorithmen aus (ASCII)
- Speichert Ergebnisse als JSON und CSV
- Berechnet: Speedup A* vs. Dijkstra, Pfadqualitätsverlust Greedy vs. Dijkstra
- Zusammenfassung in einem Satz

Aufruf:
    python benchmark_report.py --n 200 --output results/
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path

import httpx

API_BASE = "http://localhost:8000"


def fetch_benchmark(n: int) -> dict:
    """Call the /benchmark endpoint."""
    resp = httpx.get(f"{API_BASE}/benchmark", params={"n": n}, timeout=300)
    resp.raise_for_status()
    return resp.json()


def print_table(data: dict):
    """Print ASCII comparison table."""
    algos = data["algorithms"]
    n = data["n"]

    header = f"{'Metrik':<35} {'Greedy':>12} {'Dijkstra':>12} {'A*':>12}"
    divider = "─" * len(header)

    print()
    print(f"  Algorithmen-Benchmark über {n} zufällige Routenpaare")
    print(f"  {divider}")
    print(f"  {header}")
    print(f"  {divider}")

    metrics = [
        ("Ø Laufzeit (ms)", "avg_runtime_ms", ".2f"),
        ("Median Laufzeit (ms)", "median_runtime_ms", ".2f"),
        ("P95 Laufzeit (ms)", "p95_runtime_ms", ".2f"),
        ("Ø Expandierte Knoten", "avg_nodes_expanded", ".1f"),
        ("Ø Pfadkosten", "avg_path_cost", ".2f"),
        ("Optimalität vs. Dijkstra", "path_optimality_vs_dijkstra", ".4f"),
        ("Routen gefunden", "n_found", "d"),
        ("Keine Route", "n_no_path_found", "d"),
    ]

    for label, key, fmt in metrics:
        vals = []
        for algo in ["greedy", "dijkstra", "astar"]:
            v = algos[algo].get(key)
            if v is None:
                vals.append("—")
            elif fmt == "d":
                vals.append(f"{int(v)}")
            else:
                vals.append(f"{v:{fmt}}")
        print(f"  {label:<35} {vals[0]:>12} {vals[1]:>12} {vals[2]:>12}")

    print(f"  {divider}")


def save_json(data: dict, output_dir: Path):
    path = output_dir / "benchmark_results.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  JSON gespeichert: {path}")


def save_csv(data: dict, output_dir: Path):
    path = output_dir / "benchmark_results.csv"
    algos = data["algorithms"]
    fields = ["algorithm", "avg_runtime_ms", "median_runtime_ms", "p95_runtime_ms",
              "avg_nodes_expanded", "avg_path_cost", "path_optimality_vs_dijkstra",
              "n_found", "n_no_path_found"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for algo in ["greedy", "dijkstra", "astar"]:
            row = {"algorithm": algo, **algos[algo]}
            writer.writerow(row)
    print(f"  CSV gespeichert:  {path}")


def print_summary(data: dict):
    algos = data["algorithms"]
    n = data["n"]

    dij_rt = algos["dijkstra"]["avg_runtime_ms"]
    astar_rt = algos["astar"]["avg_runtime_ms"]
    greedy_opt = algos["greedy"].get("path_optimality_vs_dijkstra")

    if dij_rt > 0:
        speedup = ((dij_rt - astar_rt) / dij_rt) * 100
    else:
        speedup = 0

    if greedy_opt and greedy_opt > 0:
        greedy_deviation = (greedy_opt - 1.0) * 100
    else:
        greedy_deviation = 0

    astar_opt = algos["astar"].get("path_optimality_vs_dijkstra")
    astar_deviation = ((astar_opt - 1.0) * 100) if astar_opt else 0

    print()
    print(f"  ZUSAMMENFASSUNG:")
    print(f"  A* ist {speedup:.1f}% {'schneller' if speedup > 0 else 'langsamer'} als Dijkstra "
          f"bei {astar_deviation:.2f}% Pfadkostenabweichung über {n} Routen.")
    print(f"  Greedy weicht {greedy_deviation:.1f}% von der optimalen Dijkstra-Lösung ab.")
    print()


def main():
    parser = argparse.ArgumentParser(description="Algorithmen-Benchmark Report")
    parser.add_argument("--n", type=int, default=100, help="Anzahl Routenpaare (default: 100)")
    parser.add_argument("--output", type=str, default="results/", help="Output-Verzeichnis")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n  Starte Benchmark mit {args.n} Routenpaaren...")
    print(f"  (Das kann bei großen Graphen einige Minuten dauern.)")

    try:
        data = fetch_benchmark(args.n)
    except httpx.ConnectError:
        print("FEHLER: Backend nicht erreichbar unter", API_BASE, file=sys.stderr)
        print("Starten Sie zuerst: uvicorn app:app --port 8000", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPStatusError as e:
        print(f"FEHLER: {e.response.status_code} — {e.response.text}", file=sys.stderr)
        sys.exit(1)

    print_table(data)
    save_json(data, output_dir)
    save_csv(data, output_dir)
    print_summary(data)


if __name__ == "__main__":
    main()

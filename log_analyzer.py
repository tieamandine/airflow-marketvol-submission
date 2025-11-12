import sys
from pathlib import Path
from datetime import datetime

def analyze_file(file_path: Path):
    """
    Parse a single log file and return:
      - error_count: number of lines with 'ERROR'
      - errors: list[str] of full error lines (trimmed)
    """
    error_count = 0
    errors = []
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if "ERROR" in line:
                    error_count += 1
                    errors.append(line.rstrip())
    except Exception as e:
        errors.append(f"[ANALYZER] Could not read {file_path}: {e}")
    return error_count, errors

def analyze_logs(log_dir: str):
    """
    Recursively walk log_dir, analyze all *.log files, and
    return a dict with per-file counts and a global error list.
    """
    per_file_counts = {}
    gathered = []  # (path, line) tuples for each error
    log_files = sorted(Path(log_dir).rglob("*.log"))

    for fp in log_files:
        count, lines = analyze_file(fp)
        if count:
            per_file_counts[str(fp)] = count
            for ln in lines:
                gathered.append((str(fp), ln))

    return per_file_counts, gathered

def write_report(per_file_counts, gathered, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"log_report_{ts}.txt"

    total = sum(per_file_counts.values())
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"Airflow Log Analyzer Report\n")
        f.write(f"Generated: {datetime.now().isoformat(timespec='seconds')}\n\n")
        f.write(f"Total number of errors: {total}\n\n")

        if total == 0:
            f.write("✅ No errors found in log files.\n")
        else:
            f.write("Per-file error counts:\n")
            for path, cnt in sorted(per_file_counts.items(), key=lambda x: x[0]):
                f.write(f"  {path}: {cnt}\n")
            f.write("\nHere are all the errors:\n\n")
            for path, line in gathered:
                f.write(f"{path} :: {line}\n")

    return out_path, total

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 log_analyzer.py <path_to_log_directory>")
        sys.exit(1)

    log_dir = sys.argv[1]
    per_file_counts, gathered = analyze_logs(log_dir)

    # Console summary
    total = sum(per_file_counts.values())
    print(f"\nTotal number of errors: {total}")
    if total == 0:
        print("✅ No errors found in log files.")
    else:
        print("Per-file error counts:")
        for path, cnt in sorted(per_file_counts.items(), key=lambda x: x[0]):
            print(f"  {path}: {cnt}")
        print("\nHere are all the errors:\n")
        for path, line in gathered:
            print(f"{path} :: {line}")

    # Write report next to the script
    out_path, _ = write_report(per_file_counts, gathered, Path("."))
    print(f"\n📄 Report written to: {out_path}")

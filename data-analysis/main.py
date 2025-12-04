#!/usr/bin/env python3
"""
data-analysis/main.py

Starter script to analyze CSV outputs from the reddit scraper(s).

Features (starter):
- discover CSV files in `output_scrapers/` containing 'reddit' in the filename
- load them into a single pandas DataFrame
- normalize/parse a creation datetime column
- compute basic summaries: total posts, posts per day, top subreddits, top words
- save summary CSVs and simple plots (optional via CLI)

This is intentionally a minimal, robust starting point you can extend.
"""
from __future__ import annotations

import argparse
import glob
import os
import re
from collections import Counter
from pathlib import Path
from typing import List

import pandas as pd


def find_reddit_csvs(root: Path) -> List[Path]:
    out_dir = root / "output_scrapers"
    if not out_dir.exists():
        return []
    # Prefer files that mention 'reddit' to limit to social scraper outputs
    files = list(out_dir.glob("*.csv"))
    reddit_files = [f for f in files if "reddit" in f.name.lower() or "vw_old" in f.name.lower()]
    return reddit_files or files


def load_csvs(paths: List[Path]) -> pd.DataFrame:
    frames = []
    for p in paths:
        try:
            df = pd.read_csv(p, dtype=str, keep_default_na=False)
            df["__source_file"] = p.name
            frames.append(df)
        except Exception as e:
            print(f"[WARN] Could not read {p}: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def infer_datetime_column(df: pd.DataFrame) -> str | None:
    # common names
    candidates = [c for c in df.columns if c.lower() in ("created_utc", "created", "time", "timestamp", "date", "created_iso")]
    if candidates:
        return candidates[0]
    # fallback: search for any column that looks like a datetime string in first non-empty row
    for c in df.columns:
        sample = next((x for x in df[c].values if str(x).strip()), "")
        if sample and re.search(r"\d{4}-\d{2}-\d{2}", str(sample)):
            return c
    return None


def normalize_datetime(df: pd.DataFrame, col: str) -> pd.DataFrame:
    # Try common formats, numeric epoch fallback
    s = df[col].replace("", None)
    # If all numeric, treat as epoch seconds
    if s.dropna().apply(lambda x: str(x).isdigit()).all():
        df["created"] = pd.to_datetime(s.astype(float), unit="s", errors="coerce")
    else:
        df["created"] = pd.to_datetime(s, utc=True, errors="coerce")
    return df


def extract_subreddit(df: pd.DataFrame) -> pd.DataFrame:
    if "subreddit" in df.columns:
        return df
    # try to extract from permalink if available
    if "permalink" in df.columns:
        def get_sub(p):
            try:
                m = re.search(r"/r/([^/]+)/", p)
                return m.group(1) if m else None
            except Exception:
                return None
        df["subreddit"] = df["permalink"].apply(get_sub)
    return df


def top_words(series: pd.Series, top_n: int = 25) -> List[tuple]:
    text = " ".join([str(x) for x in series.dropna().astype(str)])
    # simple tokenization: split on non-letters, lowercase
    tokens = [t.lower() for t in re.split(r"[^a-zA-Z]+", text) if len(t) > 2]
    # simple stoplist
    stop = set(["the", "and", "for", "with", "that", "this", "from", "are", "was", "have", "you", "but", "not"])
    filtered = [t for t in tokens if t not in stop]
    cnt = Counter(filtered)
    return cnt.most_common(top_n)


def summarize(df: pd.DataFrame, out_dir: Path, do_plots: bool = True) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {}
    summary["total_rows"] = len(df)

    # posts per day
    if "created" in df.columns and not df["created"].isna().all():
        df_ts = df.dropna(subset=["created"]).copy()
        df_ts["created"] = pd.to_datetime(df_ts["created"])  # ensure tz-naive for grouping
        per_day = df_ts.groupby(pd.Grouper(key="created", freq="D")).size()
        per_day = per_day.rename("counts").reset_index()
        per_day.to_csv(out_dir / "posts_per_day.csv", index=False)
        summary["days_covered"] = per_day["created"].min().isoformat() if not per_day.empty else None
        if do_plots:
            try:
                import matplotlib.pyplot as plt

                plt.figure(figsize=(8, 3))
                plt.plot(per_day["created"], per_day["counts"], marker="o")
                plt.title("Posts per day")
                plt.tight_layout()
                plt.savefig(out_dir / "posts_per_day.png")
                plt.close()
            except Exception as e:
                print(f"[WARN] Could not create plot (matplotlib missing?): {e}")

    # top subreddits
    if "subreddit" in df.columns:
        top_subs = df["subreddit"].dropna().value_counts().reset_index()
        top_subs.columns = ["subreddit", "counts"]
        top_subs.to_csv(out_dir / "top_subreddits.csv", index=False)
        summary["unique_subreddits"] = int(top_subs["subreddit"].nunique())

    # top words in titles
    title_col = next((c for c in df.columns if c.lower() == "title"), None)
    if title_col:
        tw = top_words(df[title_col])
        pd.DataFrame(tw, columns=["word", "count"]).to_csv(out_dir / "title_word_counts.csv", index=False)

    # write summary metadata
    pd.Series(summary).to_csv(out_dir / "summary_metadata.csv")
    print(f"[INFO] Wrote analysis outputs to {out_dir}")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Basic analysis of reddit scraper CSV outputs")
    parser.add_argument("--root", default=os.path.join(os.path.dirname(__file__), ".."), help="Repo root (default: parent of this script)")
    parser.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "output"), help="Output folder for summaries and plots")
    parser.add_argument("--dry-run", action="store_true", help="Do not write output files (just print planned actions)")
    parser.add_argument("--no-plots", action="store_true", help="Skip plot generation")
    args = parser.parse_args(argv)

    repo_root = Path(args.root).resolve()
    out_dir = Path(args.out).resolve()

    files = find_reddit_csvs(repo_root)
    if not files:
        print(f"[WARN] No CSV files found in {repo_root / 'output_scrapers'}")
        return 2

    print(f"[INFO] Found {len(files)} CSV file(s): {[p.name for p in files]}")
    df = load_csvs(files)
    if df.empty:
        print("[WARN] No data loaded from CSVs")
        return 3

    dt_col = infer_datetime_column(df)
    if dt_col:
        df = normalize_datetime(df, dt_col)
        print(f"[INFO] Parsed datetimes from column: {dt_col}")
    else:
        print("[WARN] Could not infer a datetime column — time-based summaries will be skipped")

    df = extract_subreddit(df)

    if args.dry_run:
        print("[DRY-RUN] Would summarize and (optionally) write outputs to:", out_dir)
        # print a small preview
        print(df.head(3).to_dict(orient="records"))
        return 0

    summarize(df, Path(out_dir), do_plots=not args.no_plots)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
# Version 0.1
# supported files: csv, json
# lm-studio integration: yes


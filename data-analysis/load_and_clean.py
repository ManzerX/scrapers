#!/usr/bin/env python3
"""Load and clean CSV outputs from scrapers.

Writes a merged cleaned CSV to `data-analysis/output/cleaned_reddit.csv`.
"""
from pathlib import Path
import os
import re
import pandas as pd


def find_csvs(output_dir: Path):
    if not output_dir.exists():
        return []
    files = sorted(output_dir.glob('*.csv'))
    # Prefer reddit-like files
    prefer = [f for f in files if 'reddit' in f.name.lower() or 'vw_old' in f.name.lower()]
    return prefer or files


def infer_datetime_column(df: pd.DataFrame):
    candidates = [c for c in df.columns if c.lower() in ('created_utc','created','time','timestamp','date','created_iso')]
    if candidates:
        return candidates[0]
    for c in df.columns:
        sample = next((x for x in df[c].values if str(x).strip()), '')
        if sample and re.search(r"\d{4}-\d{2}-\d{2}", str(sample)):
            return c
    return None


def normalize_datetime(df: pd.DataFrame, col: str):
    s = df[col].replace('', pd.NA)
    # Try to coerce numeric epoch seconds first; use to_numeric to safely handle NA
    numeric = pd.to_numeric(s, errors='coerce')
    if numeric.notna().any():
        # treat numeric values as epoch seconds when reasonable
        df['created'] = pd.to_datetime(numeric, unit='s', errors='coerce')
        # if conversion yielded mostly NaT, fallback to parsing strings
        if df['created'].notna().sum() < max(1, int(0.5 * len(df))):
            df['created'] = pd.to_datetime(s, utc=True, errors='coerce')
    else:
        df['created'] = pd.to_datetime(s, utc=True, errors='coerce')
    return df


def load_and_clean(repo_root: Path):
    out_dir = repo_root / 'output_scrapers'
    files = find_csvs(out_dir)
    if not files:
        print(f'[WARN] No CSV files found in {out_dir}')
        return None
    print(f'[INFO] Loading {len(files)} CSV files')
    frames = []
    for p in files:
        try:
            df = pd.read_csv(p, dtype=str, keep_default_na=False)
            df['__source_file'] = p.name
            frames.append(df)
        except Exception as e:
            print(f'[WARN] Failed to read {p}: {e}')
    if not frames:
        print('[WARN] No frames loaded')
        return None
    df = pd.concat(frames, ignore_index=True, sort=False)
    # normalize empty strings to NaN
    df = df.replace({'': pd.NA})

    # infer and normalize datetime
    dt_col = infer_datetime_column(df)
    if dt_col:
        df = normalize_datetime(df, dt_col)
        print(f'[INFO] Parsed datetimes from column: {dt_col} (created column)')
    else:
        print('[WARN] Could not infer datetime column')

    # drop exact duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f'[INFO] Dropped {before-after} exact duplicate rows')

    # try to deduplicate by url if present
    if 'url' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['url'])
        after = len(df)
        print(f'[INFO] Dropped {before-after} duplicates based on url')

    # ensure output folder
    save_dir = repo_root / 'data-analysis' / 'output'
    save_dir.mkdir(parents=True, exist_ok=True)
    out_path = save_dir / 'cleaned_reddit.csv'
    df.to_csv(out_path, index=False)
    print(f'[INFO] Wrote cleaned CSV to {out_path} ({len(df)} rows)')
    return out_path


if __name__ == '__main__':
    repo = Path(__file__).resolve().parents[1]
    load_and_clean(repo)

#!/usr/bin/env python3
"""
Generate a yield-curve heatmap (Bundesbank Svensson curve) and export a PDF.

Usage (from this folder):
  python3 generate_yield_heatmap.py

Optional:
  python3 generate_yield_heatmap.py --start 2000-01 --end 2025-01
"""

from __future__ import annotations

import argparse
import io
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
import requests

# Plotting imports are separated so the script can still download/clean even if
# plotting libraries are missing.
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
except Exception as exc:  # pragma: no cover - handled at runtime
    plt = None
    sns = None


BASE_URL = "https://api.statistiken.bundesbank.de/rest/data"
FLOW_REF = "BBSIS"

# Svensson yield curve series keys (1Y to 30Y)
SVENSSON_KEYS: Dict[str, str] = {
    "1_year": "M.I.ZST.ZI.EUR.S1311.B.A604.R01XX.R.A.A._Z._Z.A",
    "2_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R02XX.R.A.A._Z._Z.A",
    "3_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R03XX.R.A.A._Z._Z.A",
    "4_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R04XX.R.A.A._Z._Z.A",
    "5_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R05XX.R.A.A._Z._Z.A",
    "6_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R06XX.R.A.A._Z._Z.A",
    "7_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R07XX.R.A.A._Z._Z.A",
    "8_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R08XX.R.A.A._Z._Z.A",
    "9_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R09XX.R.A.A._Z._Z.A",
    "10_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R10XX.R.A.A._Z._Z.A",
    "11_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R11XX.R.A.A._Z._Z.A",
    "12_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R12XX.R.A.A._Z._Z.A",
    "13_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R13XX.R.A.A._Z._Z.A",
    "14_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R14XX.R.A.A._Z._Z.A",
    "15_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R15XX.R.A.A._Z._Z.A",
    "16_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R16XX.R.A.A._Z._Z.A",
    "17_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R17XX.R.A.A._Z._Z.A",
    "18_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R18XX.R.A.A._Z._Z.A",
    "19_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R19XX.R.A.A._Z._Z.A",
    "20_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R20XX.R.A.A._Z._Z.A",
    "21_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R21XX.R.A.A._Z._Z.A",
    "22_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R22XX.R.A.A._Z._Z.A",
    "23_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R23XX.R.A.A._Z._Z.A",
    "24_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R24XX.R.A.A._Z._Z.A",
    "25_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R25XX.R.A.A._Z._Z.A",
    "26_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R26XX.R.A.A._Z._Z.A",
    "27_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R27XX.R.A.A._Z._Z.A",
    "28_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R28XX.R.A.A._Z._Z.A",
    "29_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R29XX.R.A.A._Z._Z.A",
    "30_years": "M.I.ZST.ZI.EUR.S1311.B.A604.R30XX.R.A.A._Z._Z.A",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Bundesbank Svensson yield data and build a heatmap PDF."
    )
    parser.add_argument(
        "--start",
        default="2000-01",
        help="Start period (YYYY-MM). Default: 2000-01",
    )
    parser.add_argument(
        "--end",
        default=datetime.now().strftime("%Y-%m"),
        help="End period (YYYY-MM). Default: current month",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=1.0,
        help="Seconds to pause between API calls. Default: 1.0",
    )
    parser.add_argument(
        "--lang",
        default="en",
        help="API language parameter. Default: en",
    )
    return parser.parse_args()


def validate_period(period: str) -> None:
    if not re.match(r"^\d{4}-\d{2}$", period):
        raise ValueError(f"Invalid period '{period}'. Use YYYY-MM format.")


def fetch_data_csv(
    session: requests.Session,
    flow_ref: str,
    key: str,
    start: str,
    end: str,
    lang: str,
) -> str | None:
    url = f"{BASE_URL}/{flow_ref}/{key}"
    params = {
        "startPeriod": start,
        "endPeriod": end,
        "format": "csv",
        "lang": lang,
    }
    headers = {"Accept": "text/csv"}

    try:
        response = session.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        response_text = ""
        if getattr(exc, "response", None) is not None:
            response_text = exc.response.text[:200]
        print(
            f"Failed to fetch data for key: {key}. "
            f"Error: {exc}. Message: {response_text}"
        )
        return None

    return response.text


def clean_csv_data(file_path: Path) -> pd.DataFrame:
    # Read raw text and find the first data row (YYYY-MM)
    raw_text = file_path.read_text(encoding="utf-8-sig")
    lines = raw_text.splitlines()

    data_start = None
    for idx, line in enumerate(lines):
        if re.match(r"^\d{4}-\d{2}", line):
            data_start = idx
            break

    if data_start is None:
        raise ValueError(f"No data rows found in {file_path.name}")

    data_line = lines[data_start]
    delimiter = ";" if data_line.count(";") > data_line.count(",") else ","

    data_block = "\n".join(lines[data_start:])
    df = pd.read_csv(
        io.StringIO(data_block),
        sep=delimiter,
        header=None,
    )

    # Keep only the first two columns (date + value)
    df = df.iloc[:, :2]
    df.columns = ["Date", "Value"]

    # Convert date + numeric values
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m", errors="coerce")
    df["Value"] = pd.to_numeric(
        df["Value"].astype(str).str.replace(",", "."), errors="coerce"
    )

    # Drop rows with missing values
    df = df.dropna(subset=["Date", "Value"]).sort_values("Date")
    return df


def maturity_sort_key(label: str) -> int:
    match = re.match(r"^(\d+)", label)
    return int(match.group(1)) if match else 0


def maturity_display_label(label: str) -> str:
    years = maturity_sort_key(label)
    return f"{years}Y" if years else label


def build_heatmap(
    combined_df: pd.DataFrame,
    output_path: Path,
) -> None:
    if plt is None or sns is None:
        raise RuntimeError(
            "Plotting libraries are missing. Please install matplotlib and seaborn."
        )

    heatmap_df = combined_df.copy()
    heatmap_df = heatmap_df.sort_values("Date").set_index("Date")

    # Sort columns by maturity and then transpose so maturities are on Y
    columns_sorted = sorted(heatmap_df.columns, key=maturity_sort_key)
    heatmap_df = heatmap_df[columns_sorted].T

    # Display labels for maturities and place lowest at bottom
    heatmap_df.index = [maturity_display_label(col) for col in heatmap_df.index]
    heatmap_df = heatmap_df.iloc[::-1]

    # Build chart
    sns.set_theme(style="white")
    fig, ax = plt.subplots(figsize=(14, 9), dpi=300, facecolor="white")

    ax.imshow(
        heatmap_df.values,
        cmap="RdYlBu_r",
        aspect="auto",
        interpolation="bicubic",
    )

    # Remove all labels, ticks, and title for a clean presentation
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.set_title("")

    # Remove any axis borders and add generous internal whitespace
    for spine in ax.spines.values():
        spine.set_visible(False)

    fig.subplots_adjust(left=0.30, right=0.70, top=0.70, bottom=0.30)

    fig.tight_layout()
    fig.savefig(output_path, format="png", dpi=300, facecolor="white")
    plt.close(fig)


def main() -> int:
    args = parse_args()
    validate_period(args.start)
    validate_period(args.end)

    base_dir = Path(__file__).resolve().parent
    raw_dir = base_dir / "raw_csv"
    output_dir = base_dir / "output"
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    combined_df = pd.DataFrame()

    with requests.Session() as session:
        for maturity, key in SVENSSON_KEYS.items():
            print(f"Fetching data for {maturity}...")
            data = fetch_data_csv(session, FLOW_REF, key, args.start, args.end, args.lang)
            if not data:
                print(f"Skipping {maturity} due to download error.")
                continue

            # Save raw CSV for traceability
            raw_path = raw_dir / f"yield_curve_{maturity}.csv"
            raw_path.write_text(data, encoding="utf-8")

            # Clean and merge
            cleaned = clean_csv_data(raw_path)
            cleaned.rename(columns={"Value": maturity}, inplace=True)

            if combined_df.empty:
                combined_df = cleaned
            else:
                combined_df = pd.merge(combined_df, cleaned, on="Date", how="outer")

            time.sleep(max(args.pause, 0))

    if combined_df.empty:
        print("No data downloaded. Exiting.")
        return 1

    combined_df = combined_df.sort_values("Date")
    combined_path = output_dir / "combined_yield_curve.csv"
    combined_df.to_csv(combined_path, index=False)
    print(f"Combined data saved to {combined_path}")

    heatmap_path = output_dir / "yield_curve_heatmap.png"
    build_heatmap(combined_df, heatmap_path)
    print(f"Heatmap saved to {heatmap_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

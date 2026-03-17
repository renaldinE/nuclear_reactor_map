import pandas as pd
from pathlib import Path
from typing import List


def load_and_clean_pris_file(file_path: Path, skiprows: int = 19) -> pd.DataFrame:
    """
    Load a single PRIS Excel file and clean it:
    - Skip initial metadata rows.
    - Drop rows with missing ISO Code or repeated header rows.
    - Drop columns that are entirely empty.
    """
    df = pd.read_excel(file_path, skiprows=skiprows)

    # Ensure expected column exists
    if "ISO Code" not in df.columns:
        raise KeyError(f"'ISO Code' column not found in file: {file_path}")

    # Remove empty rows and repetitive headers
    mask_valid = df["ISO Code"].notna() & (df["ISO Code"] != "ISO Code")
    df = df.loc[mask_valid].copy()

    # Remove empty columns
    df.dropna(axis=1, how="all", inplace=True)

    return df


def find_pris_files(pris_dir: Path) -> List[Path]:
    """
    Return a list of Excel files (.xls, .xlsx) in the given directory.
    """
    if not pris_dir.exists() or not pris_dir.is_dir():
        raise FileNotFoundError(f"Directory not found or not a directory: {pris_dir}")

    files = sorted(
        [p for p in pris_dir.iterdir() if p.suffix.lower() in {".xls", ".xlsx"}]
    )

    if not files:
        raise FileNotFoundError(f"No Excel files (.xls/.xlsx) found in {pris_dir}")

    return files


def merge_pris_directory(
    base_dir: Path,
    pris_subdir: str = "pris_data",
    skiprows: int = 19,
) -> pd.DataFrame:
    """
    Find all PRIS Excel files in `pris_subdir` under `base_dir`,
    load and clean them, and return one merged DataFrame.
    """
    pris_dir = base_dir / pris_subdir
    files = find_pris_files(pris_dir)

    dfs = []
    for f in files:
        print(f"Loading and cleaning: {f.name}")

        # Use a different number of header rows for specific files
        effective_skiprows = 23 if f.name == "Load Factor (LF).xlsx" else skiprows

        df = load_and_clean_pris_file(f, skiprows=effective_skiprows)
        df["source_file"] = f.name  # Track origin file
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)
    print(f"Merged {len(files)} files into {len(merged)} rows.")
    return merged

# Base directory: directory where this script lives
base_dir = Path(__file__).resolve().parent

# Merge all PRIS files in 'pris_data'
merged_df = merge_pris_directory(base_dir, pris_subdir="pris_data", skiprows=19)

# Save result
output_path = base_dir / "merged_pris_data.csv"
merged_df.to_csv(output_path, index=False)
print(f"Saved merged data to: {output_path}")

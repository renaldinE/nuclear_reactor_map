import pandas as pd
from pathlib import Path
from typing import List

def load_and_clean_pris_file(file_path: Path, skiprows: int = 19) -> pd.DataFrame:
    """
    Load and clean a single PRIS Excel file.

    Cleaning steps:
    - Skip file metadata rows.
    - Normalize source column names.
    - Keep only rows with valid 3-letter ISO codes.
    - Drop columns that are entirely empty.
    - Apply standardized headers by PRIS file type.
    - Infer dtypes and parse date columns.
    """
    def normalize_column_name(col: object) -> str:
        col_name = str(col).strip()
        if col_name.upper() == "ARGENTINA":
            return "country"
        return col_name.lower().replace(" ", "_")

    header_map = {
        "Load Factor (LF)": [
            "country",
            "iso_code",
            "unit",
            "type",
            "model",
            "reference_unit_power",
            "data_completeness",
            "load_factor",
        ],
        "Lifetime Electricity Supplied": [
            "country",
            "iso_code",
            "unit",
            "type",
            "model",
            "operator",
            "reactor_supplier",
            "constr_date",
            "grid_date",
            "commercial_date",
            "reference_unit_power",
            "annual_electr_supplied",
            "lifetime_electr_supplied",
        ],
        "Reactor Specification": [
            "country",
            "unit",
            "iso_code",
            "type",
            "model",
            "status",
            "site_name",
            "site_location",
            "owner_long_name",
            "latest_thermal_power",
            "latest_gross_electr_power",
            "original_design_net_electr_power",
            "latest_ref_unit_power_net",
            "construction_date",
            "criticality_date",
            "grid_date",
            "commercial_date",
            "suspended_operation_date",
            "susp_op_end_date",
            "permanent_shutdown_date",
            "operator_long_name",
        ],
    }

    df = pd.read_excel(file_path, skiprows=skiprows)
    df.rename(mapper=normalize_column_name, axis="columns", inplace=True)

    if "iso_code" not in df.columns:
        raise KeyError(f"'iso_code' column not found in file: {file_path}")

    # Normalize ISO codes then remove rows that are empty or repeated headers.
    iso_code = df["iso_code"].astype("string").str.strip().str.upper()
    mask_valid = iso_code.notna() & (iso_code.str.len() == 2) & (iso_code != "ISO")
    df = df.loc[mask_valid].copy()
    df["iso_code"] = iso_code.loc[mask_valid]

    df.dropna(axis=1, how="all", inplace=True)

    matching_headers = next(
        (headers for key, headers in header_map.items() if key in file_path.name),
        None,
    )
    if matching_headers is None:
        raise ValueError(f"Unknown PRIS file naming pattern: {file_path.name}")
    if len(df.columns) != len(matching_headers):
        raise ValueError(
            f"Column count mismatch for {file_path.name}: "
            f"found {len(df.columns)}, expected {len(matching_headers)}"
        )
    df.columns = matching_headers

    df = df.convert_dtypes()
    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce", format="%Y-%m-%d")

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

    dfs = {}
    for f in files:
        print(f"Loading and cleaning: {f.name}")

        # Use a different number of header rows for specific files
        if f.name == "Load Factor (LF).xlsx":
            effective_skiprows = 23
        elif f.name == "Reactor Specification.xlsx":
            effective_skiprows = 18    
        else:
            effective_skiprows = skiprows

        df = load_and_clean_pris_file(f, skiprows=effective_skiprows)
        dfs[f.stem.lower().replace(" ","_")] = df

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

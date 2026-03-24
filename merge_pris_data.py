import pandas as pd
from pathlib import Path
from typing import List
import numpy as np
import re

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
    def add_new_column(
        df_base: pd.DataFrame, df_source: pd.DataFrame, var_name: str
    ) -> pd.DataFrame:
        """
        Add `var_name` to `df_base` by matching rows on `unit`.
        Behavior intentionally mirrors the original loop:
        - if multiple matches exist in source, use the first one
        - if no match exists, assign NaN
        """
        if "unit" not in df_base.columns:
            raise KeyError("'unit' column not found in base DataFrame")
        if "unit" not in df_source.columns:
            raise KeyError("'unit' column not found in source DataFrame")
        if var_name not in df_source.columns:
            raise KeyError(f"'{var_name}' column not found in source DataFrame")

        source_map = (
            df_source[["unit", var_name]]
            .drop_duplicates(subset="unit", keep="first")
            .set_index("unit")[var_name]
        )
        df_base[var_name] = df_base["unit"].map(source_map)
        return df_base
    
    pris_dir = base_dir / pris_subdir
    files = find_pris_files(pris_dir)

    dfs = {}
    for f in files:
        print(f"Loading and cleaning: {f.name}")

        # Use a different number of header rows for specific files
        if f.stem == "Load Factor (LF)":
            effective_skiprows = 23
        elif f.stem == "Reactor Specification":
            effective_skiprows = 18    
        else:
            effective_skiprows = skiprows

        df = load_and_clean_pris_file(f, skiprows=effective_skiprows)
        dfs[f.stem.lower().replace(" ","_")] = df
    
    # Define the key-variable pairs for merging. The keys correspond to DataFrame
    # names in `dfs`, and the variables are columns to add to the merged output.
    key_var_pairs = [
        ("load_factor_(lf)", "load_factor"),
        ("lifetime_electricity_supplied", "lifetime_electr_supplied")
    ]
    
    # Start with reactor_specification, then add columns by matching on `unit`.
    df_merged = dfs["reactor_specification"]
    for key, var in key_var_pairs:
        if key in dfs:
            df_merged = add_new_column(df_merged, dfs[key], var)
        else:
            print(f"Warning: Key '{key}' not found in loaded DataFrames. Skipping variable '{var}'.")
    
    print(f"Merged {len(files)} files into {len(df_merged)} rows.")
    return df_merged

def condense_pris_data(df: pd.DataFrame) -> pd.DataFrame:
    
    pattern = r"([a-zA-Z\s.]+)-?[\d+]?"
    
    df_condensed = {
        "country": [],
        "site": [],
        "iso_code": [],
        "type": [],
        "no_reactors": [],
        "no_operational_reactors": [],
        "latest_thermal_power": [],
        "latest_gross_electr_power": [],
        "latest_ref_unit_power_net": [],
        "load_factor": [],
        "lifetime_electr_supplied": []
    }
    
    same_site = False
    no_reactors = 0
    no_op_reactors = 0
    latest_thermal_power = []
    latest_gross_electr_power = []
    latest_ref_unit_power_net = []
    load_factor = []
    lifetime_electr_supplied = 0
    for unit in df["unit"]:
        match = re.match(pattern,unit)
        mask = df["unit"] == unit
        if !(match.group(1) in df_condensed["site"]) & !(same_site):
            
            no_reactors += 1
            if df["status"] == "Operational":
                no_op_reactors += 1
                latest_thermal_power.append(df["latest_thermal_power"].loc[mask])
                latest_gross_electr_power.append(df["latest_gross_electr_power"].loc[mask])
                latest_ref_unit_power_net.append(df["latest_ref_unit_power_net"].loc[mask])
                load_factor.append(df["load_factor"].loc[mask])
                lifetime_electr_supplied += df["lifetime_electr_supplied"].loc[mask]
                
            # Turn true when new 
            same_site = True
        elif same_site:
            no_reactors += 1
            if df["status"] == "Operational":
                no_op_reactors += 1
                latest_thermal_power.append(df["latest_thermal_power"].loc[mask])
                latest_gross_electr_power.append(df["latest_gross_electr_power"].loc[mask])
                latest_ref_unit_power_net.append(df["latest_ref_unit_power_net"].loc[mask])
                load_factor.append(df["load_factor"].loc[mask])
                lifetime_electr_supplied += df["lifetime_electr_supplied"].loc[mask]
        else:
            # Store site info and average values
            df_condensed["site"].append(match.group(1))
            df_condensed["country"].append(df["country"].loc[mask])
            df_condensed["iso_code"].append(df["iso_code"].loc[mask])
            df_condensed["type"].append(df["type"].loc[mask])
            df_condensed["no_reactors"] = no_reactors
            df_condensed["no_operational_reactors"] = no_op_reactors
            df_condensed["latest_thermal_power"] = np.asarray(latest_thermal_power).mean()
            df_condensed["latest_gross_electr_power"] = np.asarray(latest_gross_electr_power).mean()
            df_condensed["latest_ref_unit_power_net"] = np.asarray(latest_ref_unit_power_net).mean()
            df_condensed["load_factor"] = np.asarray(load_factor).mean()
            df_condensed["lifetime_electr_supplied"] = lifetime_electr_supplied
            
            # Reset temporary variables
            same_site = False
            no_reactors = 0
            no_op_reactors = 0
            latest_thermal_power = []
            latest_gross_electr_power = []
            latest_ref_unit_power_net = []
            load_factor = []
            lifetime_electr_supplied = 0
        
    return pd.DataFrame(df_condensed)

# Base directory: directory where this script lives
base_dir = Path(__file__).resolve().parent

# Merge all PRIS files in 'pris_data'
merged_df = merge_pris_directory(base_dir, pris_subdir="pris_data", skiprows=19)

# Condense all PRIS files to only the NPP sites
condensed_df = condense_pris_data(merged_df)

## Save results
# Merged results
output_path = base_dir / "merged_pris_data.csv"
merged_df.to_csv(output_path, index=False)
print(f"Saved merged data to: {output_path}")

# Condensed results
output_path = base_dir / "condensed_pris_data.csv"
merged_df.to_csv(output_path, index=False)
print(f"Saved condensed data to: {output_path}")

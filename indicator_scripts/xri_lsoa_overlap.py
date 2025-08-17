from dbfread import DBF
import pandas as pd

# Paths to your five DBFs
dbf_paths = [
    r"M:\Dissertation\xRI_independent_tools\tool_outputs\8_lsoas\ndvi_LSOA_Stats_Corrected\ndvi_mosaic_lsoa_stats.dbf",
    r"M:\Dissertation\xRI_independent_tools\tool_outputs\8_lsoas\reflec_LSOA_Stats_Corrected\reflec_anomalies_mosaic_lsoa_stats.dbf",
    r"M:\Dissertation\xRI_independent_tools\tool_outputs\8_lsoas\solar_radiance_LSOA_Stats_Corrected\solar_radiance_mosaic_lsoa_stats.dbf",
    r"M:\Dissertation\xRI_independent_tools\tool_outputs\8_lsoas\temp_LSOA_Stats_Corrected\temp_anomalies_mosaic_lsoa_stats.dbf",
    r"M:\Dissertation\xRI_independent_tools\tool_outputs\8_lsoas\veg_structure_LSOA_Stats_Corrected\vegetation_structure_mosaic_lsoa_stats.dbf"
]

dfs = []

# Read each DBF and add a file-specific prefix to stat columns
for i, dbf_path in enumerate(dbf_paths, start=1):
    table = DBF(dbf_path, load=True)  # load=True reads all records into memory
    df = pd.DataFrame(iter(table))
    df = df.rename(columns={col: f"F{i}_{col}" for col in df.columns if col != "LSOA21CD"})
    dfs.append(df)

# Merge all DataFrames on LSOA21CD using an outer join
merged = dfs[0]
for df in dfs[1:]:
    merged = merged.merge(df, on="LSOA21CD", how="outer")

# Prepare lists of columns corresponding to each DBF (exclude LSOA21CD)
stat_cols = [col for col in merged.columns if col != "LSOA21CD"]
dbf_column_groups = []
start = 0
for df in dfs:
    num_cols = len(df.columns) - 1  # exclude LSOA21CD
    dbf_column_groups.append(stat_cols[start:start+num_cols])
    start += num_cols

# Count in how many DBFs each LSOA appears
def count_dbfs(row):
    count = 0
    for cols in dbf_column_groups:
        if row[cols].notnull().any():  # at least one non-null stat in this DBF
            count += 1
    return count

merged["DBF_count"] = merged.apply(count_dbfs, axis=1)

# Save to CSV for ArcGIS
merged.to_csv("merged_lsoas_correct.csv", index=False)

print("Merged CSV created: merged_lsoas_correct.csv")

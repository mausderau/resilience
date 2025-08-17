import os
import pandas as pd
from tqdm import tqdm

# --- CONFIGURATION ---
# The paths and column names should still be correct from the last script.

# 1. Path to your master address list.
MASTER_ADDRESS_FILE = r"M:\Dissertation\fresh_geocodes.csv"

# 2. The column name in your master file that contains the unique address ID (UPRN).
ADDRESS_ID_COLUMN = "UPRN"

# 3. The TOP-LEVEL folder where all your modality data is stored.
BASE_DATA_FOLDER = r"M:\Dissertation\modality_sort" 

# 4. Define the complex file requirements for each of your tools.
#    This structure now handles AND/OR logic.
#    - Each tool has a list of "requirement groups". The tool succeeds if ANY group is met (OR).
#    - Each "requirement group" is a list of substrings. The group succeeds if ALL substrings are found (AND).
TOOL_REQUIREMENTS = {
    "Vegetation_Structure": [["icp_pcd"], ["centre_pcd"]], # Success if 'icp_pcd' is found OR 'centre_pcd' is found.
    "NDVI": [["rgb", "nir", "range"]], # Success if 'rgb' AND 'nir' AND 'range' are all found.
    "Temperature_Anomalies": [["ir_temp", "range"]], # Success if 'ir_temp' AND 'range' are found.
    "Reflectance_Anomalies": [["reflec", "range"]], # Success if 'reflec' AND 'range' are found.
    "Solar_Radiance": [["icp_pcd"], ["centre_pcd"]] # Success if 'icp_pcd' is found OR 'centre_pcd' is found.
}

# 5. The full path for the final detailed output report.
OUTPUT_REPORT_CSV = r"M:\Dissertation\xRI_independent_tools\tool_outputs\audit_report_final.csv"
# ---------------------

def final_audit():
    """
    Audits addresses using complex AND/OR logic based on the specific file
    requirements for each tool.
    """
    print("Starting final audit with complex requirement logic...")

    # Step 1: Scan the directory to build a map of UPRNs to their files.
    print(f"Scanning directory '{BASE_DATA_FOLDER}' to index all files. Please wait...")
    uprn_file_map = {}
    for root, dirs, files in tqdm(os.walk(BASE_DATA_FOLDER), desc="Scanning Folders"):
        for dirname in dirs:
            if dirname.isdigit() and len(dirname) > 6:
                uprn = dirname
                if uprn not in uprn_file_map:
                    uprn_file_map[uprn] = set() # Use a set for faster lookups
                
                uprn_path = os.path.join(root, dirname)
                for sub_root, _, sub_files in os.walk(uprn_path):
                    for filename in sub_files:
                        # Store the full path for more reliable checking if needed, but filename is often enough
                        uprn_file_map[uprn].add(filename)
    
    if not uprn_file_map:
        print(f"\nERROR: Could not find any UPRN-named folders in '{BASE_DATA_FOLDER}'. Please check the path.")
        return
        
    print(f"\nScan complete. Found data for {len(uprn_file_map)} unique UPRNs.")

    # Step 2: Read the master address list
    try:
        df = pd.read_csv(MASTER_ADDRESS_FILE)
        df[ADDRESS_ID_COLUMN] = df[ADDRESS_ID_COLUMN].astype(str)
    except Exception as e:
        print(f"ERROR: Could not read master address file: {e}")
        return

    # Step 3: Prepare DataFrame and perform the detailed audit
    for tool_name in TOOL_REQUIREMENTS.keys():
        df[tool_name] = 0

    print("Auditing addresses against complex file requirements...")
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Auditing Addresses"):
        uprn = row[ADDRESS_ID_COLUMN]
        
        if uprn in uprn_file_map:
            available_files = uprn_file_map[uprn]
            
            # --- New Complex Logic ---
            for tool_name, requirement_groups in TOOL_REQUIREMENTS.items():
                tool_success = False
                # Check if ANY requirement group is satisfied (OR logic)
                for group in requirement_groups:
                    # Check if ALL substrings in this group are present (AND logic)
                    group_success = all(any(req in f for f in available_files) for req in group)
                    
                    if group_success:
                        tool_success = True
                        break # This tool's requirement is met, no need to check other OR groups
                
                if tool_success:
                    df.at[index, tool_name] = 1

    # Step 4: Summarize and save the report
    tool_columns = list(TOOL_REQUIREMENTS.keys())
    df['Tools_Available_Count'] = df[tool_columns].sum(axis=1)
    df.to_csv(OUTPUT_REPORT_CSV, index=False)

    print("\n--- Final Audit Summary ---")
    print("Number of addresses with the required data for a given number of tools:")
    summary = df['Tools_Available_Count'].value_counts().sort_index(ascending=False)
    print(summary)
    print("--------------------")
    print(f"\nSUCCESS: Detailed final audit report saved to:\n{OUTPUT_REPORT_CSV}")

if __name__ == "__main__":
    final_audit()
import sys
import os
import csv
from pathlib import Path
from tqdm import tqdm

# --- Add user site-packages to path ---
USER_SITE_PACKAGES = r'C:\Users\3018864a\AppData\Roaming\Python\Python311\site-packages'
if USER_SITE_PACKAGES not in sys.path:
    sys.path.insert(0, USER_SITE_PACKAGES)

import open3d
import arcpy

# --- CONFIGURATION ---
# EDIT THESE PATHS TO MATCH YOUR SYSTEM
ICP_PCD_ROOT = r"M:\Dissertation\modality_sort\icp_pcd"
CENTRE_PCD_ROOT = r"M:\Dissertation\modality_sort\centre_pcd"
GEOCODE_TABLE = r"M:\Dissertation\fresh_geocodes.csv"
OUTPUT_CSV = r"M:\Dissertation\xRI_independent_tools\tool_outputs\centroids_final.csv"   
# ---------------------

def find_pcd_files(root_folder, file_pattern, geocoded_uprns):
    """Finds PCD files and maps them by UPRN."""
    file_map = {}
    for root, _, files in os.walk(str(root_folder)):
        for file in files:
            if file.lower().endswith(file_pattern):
                uprn = Path(root).parent.name
                if uprn in geocoded_uprns:
                    file_map[uprn] = os.path.join(root, file)
    return file_map

def load_geocodes_with_arcpy(geocode_table_path):
    """
    Loads geocodes using arcpy.da.SearchCursor for maximum reliability with GIS tables.
    """
    coords = {}
    uprns_in_file = set()
    total_rows = 0
    
    with arcpy.da.SearchCursor(geocode_table_path, ["UPRN", "X_COORDINATE", "Y_COORDINATE"]) as cur:
        for row in cur:
            total_rows += 1
            uprn, x, y = row[0], row[1], row[2]
            
            # Use the string representation for the dictionary key
            uprn_str = str(uprn).strip()
            
            if uprn_str and x is not None and y is not None:
                coords[uprn_str] = (float(x), float(y))
                uprns_in_file.add(uprn_str)

    print(f"Total rows scanned by ArcPy: {total_rows}")
    return coords, uprns_in_file


def main():
    """Main execution function."""
    print("Starting Step 1: Calculate PCD Centroids (Final ArcPy Reader)...")
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    # Use the new ArcPy-based loading function
    coords, geocoded_uprns = load_geocodes_with_arcpy(GEOCODE_TABLE)
    
    print(f"Found {len(geocoded_uprns)} unique geocoded UPRNs.")

    # Find all PCD files
    pcd_files = find_pcd_files(ICP_PCD_ROOT, ".pcd", geocoded_uprns)
    centre_files = find_pcd_files(CENTRE_PCD_ROOT, ".pcd", geocoded_uprns)
    
    for uprn, path in centre_files.items():
        if uprn not in pcd_files:
            pcd_files[uprn] = path
    
    print(f"Total unique PCD files to process: {len(pcd_files)}")

    try:
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['UPRN', 'X', 'Y', 'Z'])
            
            uprns_to_process = {uprn: path for uprn, path in pcd_files.items() if uprn in geocoded_uprns}

            for uprn, pcd_path in tqdm(uprns_to_process.items(), desc="Calculating Centroids"):
                try:
                    pcd = open3d.io.read_point_cloud(pcd_path)
                    if not pcd.has_points():
                        continue
                    centroid = pcd.get_center()
                    writer.writerow([uprn, centroid[0], centroid[1], centroid[2]])
                except Exception as e:
                    print(f"ERROR: Could not process UPRN {uprn} at {pcd_path}: {e}")
        print(f"Centroid calculation complete. Output saved to: {OUTPUT_CSV}")
    except IOError as e:
        print(f"FATAL ERROR: Failed to write to CSV file {OUTPUT_CSV}: {e}")

if __name__ == "__main__":
    main()
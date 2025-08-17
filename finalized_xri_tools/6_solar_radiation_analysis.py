import sys
import os
import csv
from pathlib import Path
import numpy as np
from tqdm import tqdm
import traceback

# --- Add user site-packages to path ---
USER_SITE_PACKAGES = r'C:\Users\3018864a\AppData\Roaming\Python\Python311\site-packages'
if USER_SITE_PACKAGES not in sys.path:
    sys.path.insert(0, USER_SITE_PACKAGES)

import open3d
import arcpy

# --- CONFIGURATION ---
# EDIT THESE PATHS AND PARAMETERS
ICP_PCD_ROOT = r"M:\Dissertation\modality_sort\icp_pcd"
CENTRE_PCD_ROOT = r"M:\Dissertation\modality_sort\centre_pcd"
GEOCODE_TABLE = r"M:\Dissertation\fresh_geocodes.csv"
# Create two new output folders for the intermediate and final rasters
OUTPUT_DSM_FOLDER = r"M:\Dissertation\xRI_independent_tools\tool_outputs\solar_dsm"
OUTPUT_SOLAR_FOLDER = r"M:\Dissertation\xRI_independent_tools\tool_outputs\8_solar_radiation"
CELL_SIZE = 1.0

# --- Solar Analysis Parameters ---
# A more representative average latitude for England and Wales
LATITUDE = 52.5 

# Option 1: To model peak summer heat stress (Recommended for this index)
DAY_OF_YEAR = 172 # June 21st, the summer solstice
START_HOUR = 8    # 8:00 AM
END_HOUR = 18     # 6:00 PM

# Option 2: To model the winter conditions at time of capture
# DAY_OF_YEAR = 355 # December 21st, the winter solstice
# START_HOUR = 9    # 9:00 AM
# END_HOUR = 16     # 4:00 PM
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
    print("Starting Step 6: Solar Radiation Analysis...")
    os.makedirs(OUTPUT_DSM_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_SOLAR_FOLDER, exist_ok=True)
    sr = arcpy.SpatialReference(27700)
    failed_uprns = []

    try:
        arcpy.CheckOutExtension("Spatial")

        coords, geocoded_uprns = load_geocodes_with_arcpy(GEOCODE_TABLE)
        print(f"Loaded {len(geocoded_uprns)} geocodes.")

        pcd_files = find_pcd_files(ICP_PCD_ROOT, ".pcd", geocoded_uprns)
        centre_files = find_pcd_files(CENTRE_PCD_ROOT, ".pcd", geocoded_uprns)
        for uprn, path in centre_files.items():
            if uprn not in pcd_files:
                pcd_files[uprn] = path
        print(f"Found {len(pcd_files)} total PCD files to process.")
        
        # Define the time period for the analysis
        time_config = arcpy.sa.TimeWithinDay(DAY_OF_YEAR, START_HOUR, END_HOUR)

        for uprn, pcd_path in tqdm(list(pcd_files.items()), desc="Analyzing Solar Radiation"):
            temp_fc_path = os.path.join("in_memory", f"points_{uprn}")
            dsm_path = os.path.join(OUTPUT_DSM_FOLDER, f"dsm_{uprn}.tif")
            solar_path = os.path.join(OUTPUT_SOLAR_FOLDER, f"solar_{uprn}.tif")
            
            try:
                # 1. Create temporary point feature class from PCD
                pcd = open3d.io.read_point_cloud(pcd_path)
                if not pcd.has_points():
                    continue
                
                points_local = np.asarray(pcd.points)
                uprn_x, uprn_y = coords[uprn]
                points_global = points_local.copy()
                points_global[:, 0] += uprn_x
                points_global[:, 1] += uprn_y

                if arcpy.Exists(temp_fc_path): arcpy.management.Delete(temp_fc_path)
                arcpy.CreateFeatureclass_management("in_memory", os.path.basename(temp_fc_path), "POINT", has_z="ENABLED", spatial_reference=sr)
                with arcpy.da.InsertCursor(temp_fc_path, ["SHAPE@XYZ"]) as cursor:
                    for pt in points_global:
                        cursor.insertRow([(pt[0], pt[1], pt[2])])

                # 2. Create Digital Surface Model (DSM) raster
                arcpy.conversion.PointToRaster(
                    in_features=temp_fc_path,
                    value_field="Shape.Z",
                    out_rasterdataset=dsm_path,
                    cell_assignment="MAXIMUM",
                    cellsize=CELL_SIZE
                )

                # 3. Run Area Solar Radiation tool
                out_solar_radiation = arcpy.sa.AreaSolarRadiation(
                    in_surface_raster=dsm_path,
                    latitude=LATITUDE,
                    time_configuration=time_config,
                    day_interval=14, # Interval for calculating sun position
                    hour_interval=0.5 # Interval for calculating sun position
                )
                
                # 4. Save the final solar radiation raster
                out_solar_radiation.save(solar_path)

            except Exception as e:
                failed_uprns.append(f"{uprn} (Error: {e})")
            finally:
                # 5. Clean up intermediate files
                if arcpy.Exists(temp_fc_path):
                    arcpy.management.Delete(temp_fc_path)
                if arcpy.Exists(dsm_path):
                    arcpy.management.Delete(dsm_path)
    finally:
        arcpy.CheckInExtension("Spatial")

    if failed_uprns:
        log_path = os.path.join(OUTPUT_SOLAR_FOLDER, "failed_uprns_log.txt")
        print(f"\n{len(failed_uprns)} UPRNs failed. See {log_path} for details.")
        with open(log_path, 'w', encoding='utf-8') as f:
            for item in failed_uprns:
                f.write(f"{item}\n")

    print("Solar Radiation Analysis complete.")

if __name__ == "__main__":
    main()
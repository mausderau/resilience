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
ICP_PCD_ROOT = r"M:\Dissertation\modality_sort\icp_pcd"
CENTRE_PCD_ROOT = r"M:\Dissertation\modality_sort\centre_pcd"
GEOCODE_TABLE = r"M:\Dissertation\fresh_geocodes.csv"
OUTPUT_FOLDER = r"M:\Dissertation\xRI_independent_tools\tool_outputs\2_veg_structure"
CELL_SIZE = 1.0
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

def process_veg_structural(pcd_path, uprn_coord, cell_size):
    """
    Processes vegetation using a structural filter and the UPRN geocode for translation.
    """
    pcd = open3d.io.read_point_cloud(str(pcd_path))
    if not pcd.has_points():
        return "EMPTY_PCD", None

    points_local = np.asarray(pcd.points)

    ground_z_local = np.min(points_local[:, 2])
    above_ground_points_local = points_local[points_local[:, 2] > ground_z_local + 2.5]
    
    if above_ground_points_local.shape[0] < 100:
        return "NO_ABOVE_GROUND_POINTS", None

    pcd_above_ground = open3d.geometry.PointCloud()
    pcd_above_ground.points = open3d.utility.Vector3dVector(above_ground_points_local)

    remaining_pcd = pcd_above_ground
    for _ in range(5):
        _, inliers = remaining_pcd.segment_plane(distance_threshold=0.2, ransac_n=3, num_iterations=1000)
        remaining_pcd = remaining_pcd.select_by_index(inliers, invert=True)

    veg_points_local = np.asarray(remaining_pcd.points)
    if veg_points_local.size == 0:
        return "NO_COMPLEX_STRUCTURES", None

    # --- FINAL FIX: Translate local points using the building's global coordinate ---
    uprn_x, uprn_y = uprn_coord
    veg_points_global = veg_points_local.copy()
    veg_points_global[:, 0] += uprn_x
    veg_points_global[:, 1] += uprn_y
    
    # Define raster extent centered on the same UPRN coordinate
    buffer_dist = 150
    extent_min_x, extent_max_x = uprn_x - buffer_dist, uprn_x + buffer_dist
    extent_min_y, extent_max_y = uprn_y - buffer_dist, uprn_y + buffer_dist

    cols = int(np.ceil((extent_max_x - extent_min_x) / cell_size))
    rows = int(np.ceil((extent_max_y - extent_min_y) / cell_size))
    height_raster = np.full((rows, cols), -9999, dtype=np.float32)

    for p in veg_points_global:
        px, py, pz = p
        if extent_min_x <= px < extent_max_x and extent_min_y <= py < extent_max_y:
            col = int((px - extent_min_x) / cell_size)
            row = int((extent_max_y - py) / cell_size)
            if 0 <= row < rows and 0 <= col < cols:
                # Calculate height above the local ground and add a nominal Z for the building
                # This is an approximation; a true DEM would be needed for perfect Z values.
                height_above_local_ground = pz - ground_z_local
                if height_above_local_ground > height_raster[row, col]:
                    height_raster[row, col] = height_above_local_ground
    
    height_raster[height_raster < 0] = np.nan # Use -9999 for init, then remove any that weren't updated
    
    if not np.any(~np.isnan(height_raster)):
        return "NO_POINTS_IN_EXTENT", None
        
    lower_left = arcpy.Point(extent_min_x, extent_min_y)
    return height_raster, lower_left

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
    print("Starting Final Veg Script: Generate Rasters with Geocode Translation...")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    sr = arcpy.SpatialReference(27700)

    try:
        arcpy.CheckOutExtension("Spatial")
        arcpy.CheckOutExtension("3D")

        print(f"Loading geocodes from {os.path.basename(GEOCODE_TABLE)}...")
        # Use the new ArcPy-based loading function
        coords, geocoded_uprns = load_geocodes_with_arcpy(GEOCODE_TABLE)
        print(f"-> Found {len(geocoded_uprns)} unique geocoded UPRNs.")

        pcd_files = find_pcd_files(ICP_PCD_ROOT, ".pcd", list(coords.keys()))
        centre_files = find_pcd_files(CENTRE_PCD_ROOT, ".pcd", list(coords.keys()))
        for uprn, path in centre_files.items():
            if uprn not in pcd_files:
                pcd_files[uprn] = path
        
        print(f"Total unique PCD files to process: {len(pcd_files)}")

        for uprn, pcd_path in tqdm(list(pcd_files.items()), desc="Processing Structural Vegetation"):
            if uprn not in coords:
                continue
            try:
                height_data, lower_left_corner = process_veg_structural(
                    pcd_path, coords[uprn], CELL_SIZE)

                if lower_left_corner is None:
                    continue

                out_path = os.path.join(OUTPUT_FOLDER, f"veg_height_struct_{uprn}.tif")
                raster = arcpy.NumPyArrayToRaster(
                    height_data, lower_left_corner, CELL_SIZE, CELL_SIZE,
                    value_to_nodata=np.nan)
                raster.save(out_path)
                arcpy.management.DefineProjection(out_path, sr)
            
            except arcpy.ExecuteError:
                print(f"\n  -> ARCPY ERROR on UPRN {uprn}: {arcpy.GetMessages(2)}")
            except Exception as e:
                print(f"\n  -> PYTHON ERROR on UPRN {uprn}: {e}")
    finally:
        arcpy.CheckInExtension("Spatial")
        arcpy.CheckInExtension("3D")
    
    print("Structural-filtered vegetation raster generation complete.")

if __name__ == "__main__":
    main()
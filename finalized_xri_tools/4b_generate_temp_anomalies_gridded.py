import sys
import os
import csv
import json
import fnmatch
from pathlib import Path
import numpy as np
from tqdm import tqdm
import traceback

try:
    from PIL import Image
    import arcpy
    from sklearn.cluster import DBSCAN
    from scipy.stats import zscore
except ImportError as e:
    raise RuntimeError(f"A critical library is missing: {e}")
	
# --- CONFIGURATION ---
IR_TEMP_ROOT = r"M:\Dissertation\modality_sort\ir"
RANGE_ROOT = r"M:\Dissertation\modality_sort\range"
GEOCODE_TABLE = r"M:\Dissertation\fresh_geocodes.csv"
BEAM_INTRINSICS_JSON = r"M:\Dissertation\xRI_script_run\beam_intrinsics.json"
OUTPUT_FOLDER = r"M:\Dissertation\xRI_independent_tools\tool_outputs\4b_temp_anomalies_gridded"
CELL_SIZE = 1.0
# DBSCAN Parameters
GRID_RESOLUTION_METERS = 0.25 
DBSCAN_EPSILON_GRID_CELLS = 5 # Epsilon is now in grid cells, not meters
DBSCAN_MIN_SAMPLES = 10
# ---------------------

def find_files_by_uprn(root_folder, file_pattern, geocoded_uprns):
    """Finds files matching a pattern for a list of UPRNs."""
    file_map = {}
    for root, _, files in os.walk(str(root_folder)):
        for file in files:
            # Use fnmatch for more flexible pattern matching
            if fnmatch.fnmatch(file.lower(), file_pattern.lower()):
                # FIX: Go up two levels from the file's location to get the UPRN
                # (root -> timestamp folder, root.parent -> UPRN folder)
                uprn = Path(root).parent.name
                if uprn in geocoded_uprns:
                    file_map[uprn] = os.path.join(root, file)
    return file_map
def georeference_pixels_to_points(image_array, range_array, intrinsics, origin_coord, filter_mask=None):
    """Converts pixels from a panoramic image to 3D georeferenced points."""
    points = []
    img_rows, img_cols = image_array.shape
    alt_angles = np.deg2rad(intrinsics['beam_altitude_angles'])
    azi_angles = np.deg2rad(intrinsics['beam_azimuth_angles'])
    rows, cols = np.where(np.full(image_array.shape, True, dtype=bool))
    valid_indices = (range_array[rows, cols] > 0) & (range_array[rows, cols] < 100000)
    rows, cols = rows[valid_indices], cols[valid_indices]
    if rows.size == 0:
        return []
    dist_m = range_array[rows, cols].astype(np.float32) / 1000.0
    alt_rad = alt_angles[np.minimum(rows, len(alt_angles) - 1)]
    azi_rad = azi_angles[np.minimum(cols, len(azi_angles) - 1)]
    x_local = dist_m * np.cos(alt_rad) * np.cos(azi_rad)
    y_local = dist_m * np.cos(alt_rad) * np.sin(azi_rad)
    z_local = dist_m * np.sin(alt_rad)
    origin_x, origin_y = origin_coord
    points = np.column_stack((
        origin_x + x_local, origin_y + y_local, z_local, image_array[rows, cols]
    ))
    return points.tolist()

def create_raster_from_points(points, value_index, cell_size, uprn, out_folder, prefix, method="MEAN"):
    """Creates a raster from a list of points using an in-memory feature class."""
    if not points:
        return None
    sr = arcpy.SpatialReference(27700)
    temp_fc_name = f"{prefix}_points_{uprn}"
    temp_fc_path = os.path.join("in_memory", temp_fc_name)
    if arcpy.Exists(temp_fc_path): arcpy.management.Delete(temp_fc_path)
    arcpy.CreateFeatureclass_management("in_memory", temp_fc_name, "POINT", has_z="ENABLED", spatial_reference=sr)
    value_field_name = "Value"
    arcpy.AddField_management(temp_fc_path, value_field_name, "DOUBLE")
    with arcpy.da.InsertCursor(temp_fc_path, ["SHAPE@XYZ", value_field_name]) as cursor:
        for p in points:
            cursor.insertRow(((p[0], p[1], p[2]), float(p[value_index])))
    output_raster_path = str(Path(out_folder) / f"{prefix}_{uprn}.tif")
    arcpy.conversion.PointToRaster(
        temp_fc_path, value_field_name, output_raster_path, method, cellsize=cell_size)
    arcpy.management.Delete(temp_fc_path)
    return output_raster_path

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
    """Main execution function with detailed file logging."""
    print("Starting Step 4b: Generate Temperature Anomaly Rasters (Grid-Based)...")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    sr = arcpy.SpatialReference(27700)
    failed_uprns = []
    # Define the path for our new log file
    log_file_path = os.path.join(OUTPUT_FOLDER, "run_log.txt")

    # The entire process will be wrapped in this 'with' block
    with open(log_file_path, 'w', encoding='utf-8') as log_file:
        def log(message):
            """Helper function to write to the log file."""
            log_file.write(message + '\n')

        try:
            arcpy.CheckOutExtension("Spatial")
            with open(BEAM_INTRINSICS_JSON, 'r', encoding='utf-8') as f:
                intrinsics = json.load(f)['beam_intrinsics']
            
           # ADD THIS BLOCK IN ITS PLACE
            print(f"Loading geocodes from {os.path.basename(GEOCODE_TABLE)}...")
            # Use the new ArcPy-based loading function
            coords, geocoded_uprns = load_geocodes_with_arcpy(GEOCODE_TABLE)
            print(f"-> Found {len(geocoded_uprns)} unique geocoded UPRNs.")
            
            uprn_keys = list(coords.keys())
            ir_map = find_files_by_uprn(IR_TEMP_ROOT, "ir_temp*.npz", uprn_keys)
            range_map = find_files_by_uprn(RANGE_ROOT, "*.png", uprn_keys)
            matched = set(ir_map) & set(range_map)
            print(f"Found {len(matched)} UPRNs with matching thermal and range images.")
            log(f"Found {len(matched)} UPRNs with matching thermal and range images.")

            for uprn in tqdm(list(matched), desc="Processing Thermal Anomalies"):
                try:
                    temp_data = np.load(ir_map[uprn])
                    range_arr = np.array(Image.open(range_map[uprn]).convert("I"))

                    if range_arr.shape != temp_data.shape:
                        range_arr = np.array(Image.fromarray(range_arr).resize((temp_data.shape[1], temp_data.shape[0]), Image.NEAREST))

                    if np.count_nonzero(~np.isnan(temp_data)) < 100 or np.nanstd(temp_data) == 0:
                        log(f"UPRN {uprn} SKIPPED: Insufficient valid data or zero standard deviation.")
                        continue

                    anomalous_mask = np.abs(zscore(temp_data, nan_policy='omit')) > 1.5
                    origin_coord = coords[uprn]
                    anomalous_points = georeference_pixels_to_points(temp_data, range_arr, intrinsics, origin_coord, anomalous_mask)

                    if len(anomalous_points) < DBSCAN_MIN_SAMPLES:
                        log(f"UPRN {uprn} SKIPPED: Not enough anomalous points ({len(anomalous_points)}) for clustering.")
                        continue

                    anomalous_points = np.array(anomalous_points)
                    min_x, min_y = np.min(anomalous_points[:, 0]), np.min(anomalous_points[:, 1])
                    max_x, max_y = np.max(anomalous_points[:, 0]), np.max(anomalous_points[:, 1])
                    
                    cols = int(np.ceil((max_x - min_x) / GRID_RESOLUTION_METERS))
                    rows = int(np.ceil((max_y - min_y) / GRID_RESOLUTION_METERS))
                    
                    if cols == 0 or rows == 0:
                        log(f"UPRN {uprn} SKIPPED: Invalid grid dimensions ({rows}x{cols}).")
                        continue
                    
                    grid = np.full((rows, cols), -9999.0, dtype=np.float32)

                    point_xs = ((anomalous_points[:, 0] - min_x) / GRID_RESOLUTION_METERS).astype(int)
                    point_ys = ((anomalous_points[:, 1] - min_y) / GRID_RESOLUTION_METERS).astype(int)
                    point_temps = anomalous_points[:, 3]
                    
                    np.maximum.at(grid, (point_ys, point_xs), point_temps)
                    
                    grid_ys, grid_xs = np.where(grid > -9999.0)
                    grid_coords_for_dbscan = np.column_stack((grid_xs, grid_ys))

                    if grid_coords_for_dbscan.shape[0] < DBSCAN_MIN_SAMPLES:
                        log(f"UPRN {uprn} SKIPPED: Not enough valid grid cells ({grid_coords_for_dbscan.shape[0]}) for clustering.")
                        continue
                    
                    db = DBSCAN(eps=DBSCAN_EPSILON_GRID_CELLS, min_samples=DBSCAN_MIN_SAMPLES).fit(grid_coords_for_dbscan)
                    labels = db.labels_
                    
                    output_raster_array = np.full((rows, cols), -1, dtype=np.int32)
                    valid_indices = labels != -1
                    
                    if not np.any(valid_indices):
                        log(f"UPRN {uprn} SKIPPED: DBSCAN found no clusters.")
                        continue
                        
                    output_raster_array[grid_ys[valid_indices], grid_xs[valid_indices]] = labels[valid_indices] + 1

                    lower_left = arcpy.Point(min_x, min_y)
                    temp_raster = arcpy.NumPyArrayToRaster(output_raster_array, lower_left, GRID_RESOLUTION_METERS, GRID_RESOLUTION_METERS, value_to_nodata=-1)
                    
                    out_raster_path = str(Path(OUTPUT_FOLDER) / f"temp_anomaly_{uprn}.tif")
                    arcpy.management.Resample(temp_raster, out_raster_path, CELL_SIZE, "NEAREST")
                    arcpy.management.DefineProjection(out_raster_path, sr)
                    
                except Exception as e:
                    failed_uprns.append(f"{uprn} (Python Error: {e})")
        finally:
            arcpy.CheckInExtension("Spatial")

        if failed_uprns:
            log(f"\n{len(failed_uprns)} UPRNs failed with critical errors:")
            log('\n'.join(failed_uprns))

        print("Temperature Anomaly (Grid-Based) raster generation complete.")
if __name__ == "__main__":
    main()
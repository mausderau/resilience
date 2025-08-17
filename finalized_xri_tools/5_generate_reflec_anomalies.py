import sys
import os
import csv
import json
from pathlib import Path
import numpy as np
from tqdm import tqdm
import traceback

try:
    from PIL import Image
    import open3d
    import arcpy
    from scipy.stats import zscore
except ImportError as e:
    raise RuntimeError(f"A critical library is missing: {e}") from e

# --- CONFIGURATION ---
REFLEC_ROOT = r"M:\Dissertation\modality_sort\reflec"
RANGE_ROOT = r"M:\Dissertation\modality_sort\range"
GEOCODE_TABLE = r"M:\Dissertation\fresh_geocodes.csv"
BEAM_INTRINSICS_JSON = r"M:\Dissertation\xRI_script_run\beam_intrinsics.json"
OUTPUT_FOLDER = r"M:\Dissertation\xRI_independent_tools\tool_outputs\5_reflec_anomalies"
CELL_SIZE = 1.0
ZSCORE_THRESHOLD = 1.5 # Threshold to define an "anomaly"
# ---------------------

def find_files_by_uprn(root_folder, file_pattern, geocoded_uprns):
    """Finds files matching a pattern for a list of UPRNs."""
    file_map = {}
    for root, _, files in os.walk(str(root_folder)):
        for file in files:
            # Use fnmatch for pattern matching if needed, or simple endswith
            if file.lower().endswith(file_pattern):
                uprn = Path(root).parent.name
                if uprn in geocoded_uprns:
                    file_map[uprn] = os.path.join(root, file)
    return file_map
    
def georeference_pixels_to_points(image_array, range_array, intrinsics, origin_coord, filter_mask=None):
    """Converts pixels from a panoramic image to 3D georeferenced points."""
    if filter_mask is None:
        filter_mask = np.full(image_array.shape, True, dtype=bool)

    points = []
    img_rows, img_cols = image_array.shape
    alt_angles = np.deg2rad(intrinsics['beam_altitude_angles'])
    azi_angles = np.deg2rad(intrinsics['beam_azimuth_angles'])

    rows, cols = np.where(filter_mask)

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
        origin_x + x_local,
        origin_y + y_local,
        z_local,
        image_array[rows, cols]
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
    """Main execution function using Kernel Density for robustness."""
    print("Starting Step 5: Generate Reflectance Anomaly Rasters (Kernel Density)...")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    sr = arcpy.SpatialReference(27700)
    failed_uprns = []

    try:
        arcpy.CheckOutExtension("Spatial")

        with open(BEAM_INTRINSICS_JSON, 'r', encoding='utf-8') as f:
            intrinsics = json.load(f)['beam_intrinsics']
        
        coords, geocoded_uprns = load_geocodes_with_arcpy(GEOCODE_TABLE)
        print(f"Loaded {len(geocoded_uprns)} geocodes.")

        reflec_map = find_files_by_uprn(REFLEC_ROOT, ".png", geocoded_uprns)
        range_map = find_files_by_uprn(RANGE_ROOT, ".png", geocoded_uprns)
        
        matched = set(reflec_map) & set(range_map)
        print(f"Found {len(matched)} UPRNs with matching reflectance and range images.")

        for uprn in tqdm(list(matched), desc="Processing Reflectance Anomalies"):
            try:
                reflec_arr = np.array(Image.open(reflec_map[uprn]).convert("L"), dtype=np.float32)
                range_arr = np.array(Image.open(range_map[uprn]).convert("I"))

                if range_arr.shape != reflec_arr.shape:
                    range_img = Image.fromarray(range_arr).resize((reflec_arr.shape[1], reflec_arr.shape[0]), Image.NEAREST)
                    range_arr = np.array(range_img)

                if np.std(reflec_arr) == 0:
                    continue

                z_scores = zscore(reflec_arr)
                z_scores[~np.isfinite(z_scores)] = 0
                anomalous_mask = np.abs(z_scores) > ZSCORE_THRESHOLD
                
                origin_coord = coords[uprn]
                anomalous_points = georeference_pixels_to_points(z_scores, range_arr, intrinsics, origin_coord, anomalous_mask)

                if len(anomalous_points) < 2:
                    continue
                
                temp_fc_name = f"reflec_anomalies_{uprn}"
                temp_fc_path = os.path.join("in_memory", temp_fc_name)
                if arcpy.Exists(temp_fc_path): arcpy.management.Delete(temp_fc_path)
                
                arcpy.CreateFeatureclass_management("in_memory", temp_fc_name, "POINT", has_z="ENABLED", spatial_reference=sr)
                
                with arcpy.da.InsertCursor(temp_fc_path, ["SHAPE@XYZ"]) as cursor:
                    for pt in anomalous_points:
                        cursor.insertRow([(pt[0], pt[1], pt[2])])
                
                # --- FINAL FIX: Switch from PointDensity to KernelDensity ---
                out_raster_path = str(Path(OUTPUT_FOLDER) / f"reflec_anomaly_{uprn}.tif")
                # Kernel Density is more robust for sparse/collinear data
                # A search radius of 5-10 meters is a reasonable starting point
                out_kernel_density = arcpy.sa.KernelDensity(
                    in_features=temp_fc_path,
                    population_field="NONE",
                    cell_size=CELL_SIZE,
                    search_radius=5,
                    area_unit_scale_factor="SQUARE_METERS"
                )
                out_kernel_density.save(out_raster_path)
                # --- End of FIX ---
                
                arcpy.management.Delete(temp_fc_path)

            except arcpy.ExecuteError:
                failed_uprns.append(f"{uprn} (ArcPy Error: {arcpy.GetMessages(2)})")
            except Exception as e:
                failed_uprns.append(f"{uprn} (Python Error: {e})")
    finally:
        arcpy.CheckInExtension("Spatial")

    if failed_uprns:
        log_path = os.path.join(OUTPUT_FOLDER, "failed_uprns_log.txt")
        print(f"\n{len(failed_uprns)} UPRNs failed. See {log_path} for details.")
        with open(log_path, 'w', encoding='utf-8') as f:
            for item in failed_uprns:
                f.write(f"{item}\n")

    print("Reflectance Anomaly raster generation complete.")

if __name__ == "__main__":
    main()
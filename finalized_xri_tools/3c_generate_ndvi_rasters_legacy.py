import sys
import os
import csv
import json
from pathlib import Path
import numpy as np
from tqdm import tqdm
import traceback

try:
    # Use the base Image library from Pillow
    from PIL import Image
    import arcpy
except ImportError as e:
    raise RuntimeError(f"A critical library is missing: {e}") from e

# --- CONFIGURATION ---
RGB_ROOT = r"M:\Dissertation\modality_sort\rgb"
NIR_ROOT = r"M:\Dissertation\modality_sort\nearir"
RANGE_ROOT = r"M:\Dissertation\modality_sort\range"
GEOCODE_TABLE = r"M:\Dissertation\fresh_geocodes.csv"
BEAM_INTRINSICS_JSON = r"M:\Dissertation\xRI_script_run\beam_intrinsics.json"
OUTPUT_FOLDER = r"M:\Dissertation\xRI_independent_tools\tool_outputs\3_ndvi_fresh"
CELL_SIZE = 1.0
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

def calculate_ndvi(red_band, nir_band):
    """Calculates NDVI from red and NIR numpy arrays."""
    red = red_band.astype(np.float32)
    nir = nir_band.astype(np.float32)
    denominator = nir + red
    denominator[denominator == 0] = 1e-8
    return (nir - red) / denominator

def georeference_pixels_to_points(image_array, range_array, intrinsics, origin_coord):
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
    """Main execution function."""
    print("Starting Step 3c: Generate NDVI Rasters (Legacy Pillow version)...")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    try:
        arcpy.CheckOutExtension("Spatial")

        with open(BEAM_INTRINSICS_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # --- FIX: Access the nested "beam_intrinsics" object ---
        if 'beam_intrinsics' in data:
            intrinsics = data['beam_intrinsics']
        else:
            raise KeyError(f"The key 'beam_intrinsics' was not found in {BEAM_INTRINSICS_JSON}")

        # Validate the JSON file structure immediately
        required_keys = ['beam_altitude_angles', 'beam_azimuth_angles']
        if not all(key in intrinsics for key in required_keys):
            raise ValueError(f"The '{BEAM_INTRINSICS_JSON}' file is missing required keys.")
        print("Beam intrinsics file loaded and validated successfully.")

        print(f"Loading geocodes from {os.path.basename(GEOCODE_TABLE)}...")
        # Use the new ArcPy-based loading function
        coords, geocoded_uprns = load_geocodes_with_arcpy(GEOCODE_TABLE)
        print(f"-> Found {len(geocoded_uprns)} unique geocoded UPRNs.")

        uprn_keys = list(coords.keys())
        rgb_map = find_files_by_uprn(RGB_ROOT, ".jpeg", uprn_keys)
        nir_map = find_files_by_uprn(NIR_ROOT, ".png", uprn_keys)
        range_map = find_files_by_uprn(RANGE_ROOT, ".png", uprn_keys)
        
        matched = set(rgb_map) & set(nir_map) & set(range_map)
        print(f"Found {len(matched)} UPRNs with all required images.")

        for uprn in tqdm(list(matched), desc="Processing NDVI"):
            try:
                rgb_img = Image.open(rgb_map[uprn])
                nir_img = Image.open(nir_map[uprn])
                range_img = Image.open(range_map[uprn])

                if nir_img.size != rgb_img.size:
                    nir_img = nir_img.resize(rgb_img.size, Image.NEAREST)
                if range_img.size != rgb_img.size:
                    range_img = range_img.resize(rgb_img.size, Image.NEAREST)

                rgb_arr = np.array(rgb_img)
                nir_arr = np.array(nir_img)
                range_arr = np.array(range_img)

                ndvi_array = calculate_ndvi(rgb_arr[:, :, 0], nir_arr)
                
                origin_coord = coords[uprn]
                ndvi_points = georeference_pixels_to_points(ndvi_array, range_arr, intrinsics, origin_coord)

                if ndvi_points:
                    create_raster_from_points(ndvi_points, 3, CELL_SIZE, uprn, OUTPUT_FOLDER, "NDVI", "MEAN")

            except Exception as e:
                print(f"\n-> PYTHON ERROR on UPRN {uprn}: {e}\n{traceback.format_exc()}")
    finally:
        arcpy.CheckInExtension("Spatial")

    print("NDVI raster generation (Legacy Pillow version) complete.")

if __name__ == "__main__":
    main()
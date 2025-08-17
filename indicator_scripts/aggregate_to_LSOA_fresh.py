import arcpy
import os
import time
from collections import defaultdict

def main():
    """
    Main function to orchestrate the spatial processing and aggregation of indicator data to LSOA boundaries.
    """
    start_time = time.time()
    print("Starting script...")

    # --- Environment Settings ---
    # Set workspace, overwrite permission, and output coordinate system (BNG)
    arcpy.env.workspace = r"M:\Dissertation\indicators\output"
    arcpy.env.overwriteOutput = True
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(27700)  # British National Grid (BNG)

    # --- Input and Output Paths ---
    lsoa_fc = r"M:\Dissertation\LSOA_boundaries\LSOA_2021_EW_BSC_V4.shp"
    output_folder = r"M:\Dissertation\indicators\output"
    # Define and create a File Geodatabase for the output
    output_gdb = os.path.join(output_folder, "Indicators.gdb") 
    output_lsoa = os.path.join(output_gdb, "LSOA_with_indicators") # No .shp extension
    output_csv = os.path.join(output_folder, "LSOA_with_indicators.csv")
    
    # LSOA unique identifier field
    lsoa_join_field = "LSOA21CD"

    # --- Data Dictionaries ---

    # Data files requiring coordinate-to-point conversion
    # Added 'epsg' key for source coordinate system. 4326 = WGS84 (lat/lon)
    data_files = {
    "art_venues": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\art_venues_geocoded_real.csv",
        "x_field": "longitude", "y_field": "latitude", "epsg": 4326,
        "out_field": "ArtVenCnt"
    },
    "nuclear_sites": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\nuclear_sites.csv", # ✅ Corrected path
        "x_field": "Longitude", "y_field": "Latitude", "epsg": 4326,
        "out_field": "NuclearCnt"
    },
    "special_sites": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\special_sites_geocoded.csv",
        "x_field": "X", "y_field": "Y", "epsg": 4326, # Changed from 27700 to 4326
        "out_field": "SpcSiteCnt" 
    },
    "lending": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\lending_geocoded.csv",
        "x_field": "X", "y_field": "Y", "epsg": 27700,
        "value_field": "Loan_21_24",
        "out_field": "LoanValSum"
    }
}

    # Shapefile/vector data for direct processing
    vector_data = {
    "naptan_stops": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\naptan_stops_bng\naptan_stops_bng.shp",
        "type": "point_count", "out_field": "NaptanCnt"
    },
    "historic_landfill": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\Historic_Landfill_Sites\Historic_Landfill_SitesPolygon.shp",
        "type": "polygon_count", "out_field": "LFillCnt"  # Shortened to 8 characters
    },
    "greenspace": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\greenspace\GreenspaceSite_Merge.shp",
        "type": "percent_area", "out_field": "GreenPct"
    },
    "flood_risk": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\flood_risk_areas\flood_risk_areas_dissolve.shp",
        "type": "percent_area_dissolve", # Changed type
        "out_field": "FloodPct"
    },
    "radon": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\radon_indicative_atlas\Radon_Indicative_Atlas_v3.shp",
        "type": "percent_area", "out_field": "RadonPct",
        "query": "CLASS_MAX >= 3"
    },
    "major_roads": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\major_roads\Major_Road_Network_2018_Open_Roads.shp",
        "type": "buffered_presence", # Changed type
        "out_field": "RoadAccess2km" # Changed field name to be more descriptive
    },
    "retail": {
        "path": r"M:\Dissertation\indicators\aggregate_to_lsoa\Retail_Boundaries_UK.gpkg\main.Retail_Boundaries_UK",
        "type": "percent_area", "out_field": "RetailPct"
    }
}

    # --- Main Processing Steps ---
    # 0. Create the output File Geodatabase if it doesn't exist
    if not arcpy.Exists(output_gdb):
        arcpy.management.CreateFileGDB(os.path.dirname(output_gdb), os.path.basename(output_gdb))
        print(f"Created File Geodatabase: {output_gdb}")
    
    
    # 1. Prepare master LSOA layer
    print(f"Copying LSOA boundaries to {output_lsoa}")
    arcpy.management.CopyFeatures(lsoa_fc, output_lsoa)
    
    print("Calculating LSOA area in square kilometers...")
    lsoa_area_field = "AreaSQKM"
    arcpy.management.AddField(output_lsoa, lsoa_area_field, "DOUBLE")
    arcpy.management.CalculateGeometryAttributes(output_lsoa, [[lsoa_area_field, "AREA_GEODESIC"]], area_unit="SQUARE_KILOMETERS")

    # 2. Process table-based point data (CSV/Excel)
    for name, params in data_files.items():
        print(f"\n--- Processing: {name} ---")
        # Create a temporary in-memory feature class for the points
        point_fc = f"in_memory/{name}_points"
        
        # Convert table to point feature class, specifying the source coordinate system
        source_sr = arcpy.SpatialReference(params['epsg'])
        arcpy.management.XYTableToPoint(params["path"], point_fc, params["x_field"], params["y_field"], coordinate_system=source_sr)
        print(f"Converted {name} table to points.")

        # If it's the lending data, summarize the loan values
        if name == "lending":
            summarize_by_sum(output_lsoa, lsoa_join_field, point_fc, params["value_field"], params["out_field"])
        # Otherwise, perform a standard point count
        else:
            summarize_by_count(output_lsoa, lsoa_join_field, point_fc, params["out_field"])

    # 3. Process vector data layers
    for name, params in vector_data.items():
        print(f"\n--- Processing: {name} ---")
        query = params.get("query")

        if params["type"] == "point_count":
            summarize_by_count(output_lsoa, lsoa_join_field, params["path"], params["out_field"])
        
        elif params["type"] == "polygon_count":
            summarize_by_count(output_lsoa, lsoa_join_field, params["path"], params["out_field"])

        elif params["type"] == "percent_area":
            calculate_percent_area(output_lsoa, lsoa_join_field, lsoa_area_field, params["path"], params["out_field"], query, dissolve_first=False)
        
        elif params["type"] == "percent_area_dissolve": # New condition
            calculate_percent_area(output_lsoa, lsoa_join_field, lsoa_area_field, params["path"], params["out_field"], query, dissolve_first=True)

        elif params["type"] == "line_density":
            calculate_line_density(output_lsoa, lsoa_join_field, lsoa_area_field, params["path"], params["out_field"])
            
        elif params["type"] == "buffered_presence": # New condition
             calculate_buffered_presence(output_lsoa, lsoa_join_field, params["path"], params["out_field"], buffer_distance="2 Kilometers")
    
    # 4. Export the final attribute table to CSV
    print(f"\nExporting final attributes to {output_csv}...")
    export_to_csv(output_lsoa, lsoa_join_field, output_csv)

    end_time = time.time()
    print(f"\nAll processing complete. Total time: {end_time - start_time:.2f} seconds.")

# --- Helper Functions ---

def add_and_zero_field(target_fc, field_name, field_type="DOUBLE"):
    """
    Adds a field and initializes its values to 0. 
    This version uses a 2-step process to work around environment-specific errors.
    """
    # Check if the field already exists
    if field_name not in [f.name for f in arcpy.ListFields(target_fc)]:
        print(f"Adding field: {field_name}")
        # Step 1: Add the field (without the problematic 'default_value' parameter)
        arcpy.management.AddField(target_fc, field_name, field_type)

        # Step 2: Use CalculateField to set the new field's value to 0
        print(f"Initializing field {field_name} to 0.")
        arcpy.management.CalculateField(target_fc, field_name, 0, "PYTHON3")
        return True
    else:
        # This part handles cases where the field exists but might have null values
        print(f"Field {field_name} already exists. Populating nulls with 0.")
        with arcpy.da.UpdateCursor(target_fc, [field_name], f"{field_name} IS NULL") as cursor:
            for row in cursor:
                row[0] = 0
                cursor.updateRow(row)
        return False

def summarize_by_count(target_fc, join_field, summary_fc, out_field_name):
    """
    Counts points or polygons within each target polygon using a spatial join.
    """
    print(f"Counting features from {os.path.basename(summary_fc)} for each LSOA...")
    add_and_zero_field(target_fc, out_field_name, "LONG")
    
    # Perform the spatial join, which creates a 'Join_Count' field
    temp_join = "in_memory/temp_spatial_join"
    arcpy.analysis.SpatialJoin(target_fc, summary_fc, temp_join, "JOIN_ONE_TO_ONE", "KEEP_ALL", match_option="INTERSECT")
    
    # Create a dictionary of {LSOA_ID: Count} from the join result
    count_dict = {row[0]: row[1] for row in arcpy.da.SearchCursor(temp_join, [join_field, "Join_Count"]) if row[1] > 0}
    
    # Update the target feature class with the counts
    with arcpy.da.UpdateCursor(target_fc, [join_field, out_field_name]) as cursor:
        for row in cursor:
            if row[0] in count_dict:
                row[1] = count_dict[row[0]]
                cursor.updateRow(row)
    
    arcpy.management.Delete(temp_join)
    print(f"Successfully calculated: {out_field_name}")

def summarize_by_sum(target_fc, join_field, summary_fc, value_field, out_field_name):
    """
    Calculates the sum of a numeric field and gracefully handles cases where
    no spatial intersections are found.
    """
    print(f"Summing '{value_field}' from {os.path.basename(summary_fc)} for each LSOA...")

    # Step 1: Perform the spatial join to a temporary in-memory feature class
    field_mappings = arcpy.FieldMappings()
    field_mappings.addTable(target_fc)
    value_field_map = arcpy.FieldMap()
    value_field_map.addInputField(summary_fc, value_field)
    value_field_map.mergeRule = "Sum"
    value_field_map.outputField.name = out_field_name
    field_mappings.addFieldMap(value_field_map)

    temp_join = "in_memory/temp_sum_join"
    arcpy.analysis.SpatialJoin(target_fc, summary_fc, temp_join, "JOIN_ONE_TO_ONE", "KEEP_ALL", 
                               field_mapping=field_mappings, match_option="INTERSECT")

    # Step 2: Check if the join created the output field. If not, no matches were found.
    temp_fields = [f.name for f in arcpy.ListFields(temp_join)]
    
    if out_field_name in temp_fields:
        print("Creating summary dictionary from join results...")
        sum_dict = {row[0]: row[1] for row in arcpy.da.SearchCursor(temp_join, [join_field, out_field_name]) if row[1] is not None}
    else:
        print(f"WARNING: No spatial intersections found for this dataset. Output for '{out_field_name}' will be zero.")
        sum_dict = {} # If no field was created, create an empty dictionary.

    arcpy.management.Delete(temp_join) # Clean up the temporary data

    # Step 3: Add the field to the main LSOA layer
    print(f"Adding field '{out_field_name}' to the main layer.")
    add_and_zero_field(target_fc, out_field_name, "DOUBLE")

    # Step 4: Populate the new field with the summed values from the dictionary
    print(f"Populating '{out_field_name}' with summed values.")
    with arcpy.da.UpdateCursor(target_fc, [join_field, out_field_name]) as cursor:
        for row in cursor:
            if row[0] in sum_dict:
                row[1] = sum_dict[row[0]]
                cursor.updateRow(row)
    
    print(f"Successfully calculated: {out_field_name}")

def calculate_percent_area(target_fc, join_field, area_field, summary_fc, out_field_name, query=None, dissolve_first=False):
    """
    Calculates percentage area. MODIFIED to replace the arcpy.analysis.Statistics tool
    with a more robust Python dictionary-based summary to avoid 999999 errors.
    """
    print(f"Calculating percent area of {os.path.basename(summary_fc)}...")
    add_and_zero_field(target_fc, out_field_name, "DOUBLE")

    proc_fc = summary_fc
    if query:
        print(f"Applying filter: {query}")
        proc_fc = arcpy.management.MakeFeatureLayer(summary_fc, "in_memory/filtered_layer", query)

    if dissolve_first:
        print("Dissolving input layer to remove overlaps...")
        dissolved_fc = "in_memory/dissolved_layer"
        arcpy.management.Dissolve(proc_fc, dissolved_fc)
        proc_fc = dissolved_fc
    
    temp_intersect = "in_memory/temp_intersect_area"
    print("Intersecting layers...")
    arcpy.analysis.PairwiseIntersect([target_fc, proc_fc], temp_intersect)

    intersect_area_field = "IntAreaSKM"
    arcpy.management.AddField(temp_intersect, intersect_area_field, "DOUBLE")
    arcpy.management.CalculateGeometryAttributes(temp_intersect, [[intersect_area_field, "AREA_GEODESIC"]], area_unit="SQUARE_KILOMETERS")

    # --- NEW LOGIC: Summarize areas using a Python dictionary ---
    print("Summarizing areas using a Python dictionary...")
    sum_dict = defaultdict(float)
    with arcpy.da.SearchCursor(temp_intersect, [join_field, intersect_area_field]) as cursor:
        for lsoa_id, area in cursor:
            if lsoa_id and area:
                sum_dict[lsoa_id] += area

    # The rest of the function populates the final values from the dictionary
    with arcpy.da.UpdateCursor(target_fc, [join_field, area_field, out_field_name]) as cursor:
        for row in cursor:
            lsoa_id = row[0]
            total_area = row[1]
            if lsoa_id in sum_dict and total_area > 0:
                intersected_area = sum_dict[lsoa_id]
                row[2] = (intersected_area / total_area) * 100
                cursor.updateRow(row)

    # Clean up temporary data
    arcpy.management.Delete(temp_intersect)
    if query or dissolve_first:
        if arcpy.Exists(proc_fc): arcpy.management.Delete(proc_fc)
        
    print(f"Successfully calculated: {out_field_name}")

def calculate_line_density(target_fc, join_field, area_field, line_fc, out_field_name):
    """
    Calculates the density of lines (km per sq km) within each target polygon.
    """
    print(f"Calculating line density of {os.path.basename(line_fc)}...")
    add_and_zero_field(target_fc, out_field_name, "DOUBLE")

    # Intersect the lines with the polygons to get segments within each LSOA
    temp_intersect = "in_memory/temp_line_intersect"
    arcpy.analysis.PairwiseIntersect([target_fc, line_fc], temp_intersect)

    # Calculate the length of each intersected line segment in kilometers
    length_field_km = "Length_KM"
    arcpy.management.AddField(temp_intersect, length_field_km, "DOUBLE")
    arcpy.management.CalculateGeometryAttributes(temp_intersect, [[length_field_km, "LENGTH_GEODESIC"]], "KILOMETERS")

    # Summarize the total line length for each LSOA
    stats_table = "in_memory/line_stats"
    arcpy.analysis.Statistics(temp_intersect, stats_table, [[length_field_km, "SUM"]], join_field)

    # Create a dictionary of {LSOA_ID: SumOfLengths}
    sum_len_dict = {row[0]: row[1] for row in arcpy.da.SearchCursor(stats_table, [join_field, f"SUM_{length_field_km}"])}

    # Update the target feature class with the density (length / area)
    with arcpy.da.UpdateCursor(target_fc, [join_field, area_field, out_field_name]) as cursor:
        for row in cursor:
            lsoa_id = row[0]
            total_area = row[1]
            if lsoa_id in sum_len_dict and total_area > 0:
                total_length = sum_len_dict[lsoa_id]
                row[2] = total_length / total_area
                cursor.updateRow(row)
    
    arcpy.management.Delete(temp_intersect)
    arcpy.management.Delete(stats_table)
    print(f"Successfully calculated: {out_field_name}")

def calculate_buffered_presence(target_fc, join_field, summary_fc, out_field_name, buffer_distance="1 Kilometers"):
    """
    Determines if a target polygon is within a specified buffer distance of a summary feature.
    Outputs a binary 1 (present) or 0 (absent).
    """
    print(f"Calculating presence of {os.path.basename(summary_fc)} within {buffer_distance}...")

    # Buffer the summary features
    buffered_fc = "in_memory/buffered_features"
    arcpy.analysis.Buffer(summary_fc, buffered_fc, buffer_distance, "FULL", "ROUND", "ALL")

    # Spatially join the buffered features to the target polygons
    temp_join = "in_memory/temp_presence_join"
    arcpy.analysis.SpatialJoin(target_fc, buffered_fc, temp_join, "JOIN_ONE_TO_ONE", "KEEP_ALL", match_option="INTERSECT")
    
    # Add the binary output field
    add_and_zero_field(target_fc, out_field_name, "SHORT")
    
    # Create a set of LSOA IDs that were intersected by a buffer
    # Use Join_Count which is automatically created by Spatial Join
    presence_dict = {row[0] for row in arcpy.da.SearchCursor(temp_join, [join_field, "Join_Count"]) if row[1] > 0}
    
    # Update the target feature class (1 if present, 0 if absent)
    with arcpy.da.UpdateCursor(target_fc, [join_field, out_field_name]) as cursor:
        for row in cursor:
            if row[0] in presence_dict:
                row[1] = 1
                cursor.updateRow(row)
                
    arcpy.management.Delete(buffered_fc, temp_join)
    print(f"Successfully calculated: {out_field_name}")
    
    
def export_to_csv(fc, key_field, out_csv_path):
    """
    Exports the attributes of a feature class to a CSV file.
    """
    try:
        # Get all field names except Shape and OID
        fields = [f.name for f in arcpy.ListFields(fc) if f.type != 'Geometry' and f.name != 'OBJECTID']
        
        # Ensure the key field is the first column for clarity
        if key_field in fields:
            fields.remove(key_field)
            fields.insert(0, key_field)

        # Use a search cursor to read rows and write to CSV
        with open(out_csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = arcpy.da.SearchCursor(fc, fields)
            # Write header
            f.write(','.join(fields) + '\n')
            # Write rows
            for row in writer:
                f.write(','.join([str(v) if v is not None else '' for v in row]) + '\n')
        print(f"Successfully exported data to {out_csv_path}")
    except Exception as e:
        print(f"Error exporting to CSV: {e}")


if __name__ == "__main__":
    main()
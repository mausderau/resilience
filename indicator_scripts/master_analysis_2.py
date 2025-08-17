import arcpy
import os
import time
from collections import defaultdict

# --- Environment Settings ---
arcpy.env.overwriteOutput = True
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(27700)  # British National Grid

# --- Helper Functions ---

def add_and_zero_field(target_fc, field_name, field_type="DOUBLE"):
    """Adds a field and initializes its values to 0 if it doesn't exist."""
    field_names = [f.name for f in arcpy.ListFields(target_fc)]
    if field_name not in field_names:
        print(f"  Adding field: {field_name}")
        arcpy.management.AddField(target_fc, field_name, field_type)
        arcpy.management.CalculateField(target_fc, field_name, 0, "PYTHON3")
    else:
        print(f"  Field {field_name} already exists.")

def summarize_in_neighbourhood(target_fc, join_field, summary_fc, out_field_name, analysis_type='COUNT', weight_field=None, neighbourhood_dist="100 Meters"):
    """
    Performs a count or sum of features within each target feature's "neighbourhood"
    (the feature itself plus a buffer). This version is robust to no-match scenarios.
    """
    # Use the layer's name property to handle both paths and layer objects
    summary_fc_name = arcpy.Describe(summary_fc).name
    print(f"  Analyzing {summary_fc_name} in {neighbourhood_dist} neighbourhood...")
    
    neighbourhood_zones = "in_memory/neighbourhood_zones"
    arcpy.analysis.Buffer(target_fc, neighbourhood_zones, neighbourhood_dist, "FULL", "ROUND", "ALL")
    
    summary_layer = arcpy.management.MakeFeatureLayer(summary_fc, "in_memory/summary_layer").getOutput(0)
    arcpy.management.SelectLayerByLocation(summary_layer, "INTERSECT", neighbourhood_zones)
    
    temp_join = "in_memory/temp_neighbourhood_join"
    
    if analysis_type.upper() == 'COUNT':
        arcpy.analysis.SpatialJoin(target_fc, summary_layer, temp_join, "JOIN_ONE_TO_ONE", "KEEP_ALL", match_option="INTERSECT")
    elif analysis_type.upper() == 'SUM':
        field_mappings = arcpy.FieldMappings()
        field_mappings.addTable(target_fc)
        value_map = arcpy.FieldMap()
        value_map.addInputField(summary_layer, weight_field)
        value_map.mergeRule = "Sum"
        value_map.outputField.name = out_field_name
        field_mappings.addFieldMap(value_map)
        arcpy.analysis.SpatialJoin(target_fc, summary_layer, temp_join, "JOIN_ONE_TO_ONE", "KEEP_ALL", field_mapping=field_mappings, match_option="INTERSECT")

    summary_dict = {}
    temp_fields = [f.name for f in arcpy.ListFields(temp_join)]
    field_to_check = "Join_Count" if analysis_type.upper() == 'COUNT' else out_field_name
    
    if field_to_check in temp_fields:
        summary_dict = {row[0]: row[1] for row in arcpy.da.SearchCursor(temp_join, [join_field, field_to_check]) if row[1] is not None and row[1] > 0}
    else:
        print(f"  WARNING: No spatial intersections found for {summary_fc_name}. Output will be zero.")
    
    add_and_zero_field(target_fc, out_field_name, "LONG" if analysis_type.upper() == 'COUNT' else "DOUBLE")
    with arcpy.da.UpdateCursor(target_fc, [join_field, out_field_name]) as cursor:
        for row in cursor:
            if row[0] in summary_dict:
                row[1] = summary_dict[row[0]]
                cursor.updateRow(row)

    arcpy.management.Delete([neighbourhood_zones, summary_layer, temp_join])
    print(f"  Successfully calculated: {out_field_name}")

def calculate_percent_area(target_fc, join_field, area_field, summary_fc, out_field_name, query=None):
    """Calculates percentage area using the robust Intersect-then-Dissolve method."""
    summary_fc_name = arcpy.Describe(summary_fc).name
    print(f"  Calculating percent area of {summary_fc_name}...")
    add_and_zero_field(target_fc, out_field_name, "DOUBLE")
    
    proc_fc = summary_fc
    if query:
        proc_fc = arcpy.management.MakeFeatureLayer(summary_fc, "in_memory/filtered_layer", query).getOutput(0)
    
    dissolved_input = "in_memory/dissolved_input"
    arcpy.management.Dissolve(proc_fc, dissolved_input)
    
    intersect_pieces = "in_memory/intersect_pieces"
    arcpy.analysis.Intersect([target_fc, dissolved_input], intersect_pieces, "ALL")
    
    dissolved_intersections = "in_memory/dissolved_intersections"
    arcpy.management.Dissolve(intersect_pieces, dissolved_intersections, join_field)
    
    intersect_area_field = "IntAreaSKM"
    arcpy.management.AddField(dissolved_intersections, intersect_area_field, "DOUBLE")
    arcpy.management.CalculateGeometryAttributes(dissolved_intersections, [[intersect_area_field, "AREA_GEODESIC"]], area_unit="SQUARE_KILOMETERS")
    
    sum_dict = {row[0]: row[1] for row in arcpy.da.SearchCursor(dissolved_intersections, [join_field, intersect_area_field])}
    
    with arcpy.da.UpdateCursor(target_fc, [join_field, area_field, out_field_name]) as cursor:
        for row in cursor:
            lsoa_id = row[0]
            total_area = row[1]
            if lsoa_id in sum_dict and total_area > 0:
                intersected_area = sum_dict[lsoa_id]
                row[2] = (intersected_area / total_area) * 100
                cursor.updateRow(row)
    
    arcpy.management.Delete([intersect_pieces, dissolved_intersections, dissolved_input])
    if query:
        if arcpy.Exists(proc_fc): arcpy.management.Delete(proc_fc)
        
    print(f"  Successfully calculated: {out_field_name}")


# --- Main Function ---
def main():
    start_time = time.time()
    print("Starting MASTER analysis script...")

    # --- (1) USER-DEFINED PARAMETERS ---
    # -- Paths --
    lsoa_source_shp = r"M:\Dissertation\LSOA_boundaries\LSOA_2021_EW_BSC_V4.shp"
    output_folder = r"M:\Dissertation\indicators\output"
    population_data_csv = r"M:\Dissertation\indicators\aggregate_to_lsoa\population_count_lsoa.csv"
    output_gdb = os.path.join(output_folder, "Master_Indicators_v2.gdb")
    
    # -- Field Names from Your Data --
    # **IMPORTANT**: Verify these by opening your data's attribute tables.
    population_field_in_csv = "Census_Count"
    nuclear_status_field = "Status" 
    special_sites_area_field = "USER_Land_area__ha_" 
    retail_class_field = "Classification" 
    landfill_site_name_field = "site_name"
    flood_risk_class_field = "frr_cycle" 
    radon_class_field = "CLASS_MAX"

 # -- Weighting Schemes --
    nuclear_weights = {
        "high": ['Operational', 'Defuelling', 'Under Construction', 'Proposed New Build'],
        "medium": ['Decommissioning', 'Under Decommissioning', 'Permanent Shutdown']
    }
    retail_weights = {
        'Small Local Centre': 1, 'Local Centre': 2, 'Small Retail Park': 2,
        'District Centre': 3, 'Town Centre': 3, 'Market Town': 3,
        'Large Retail Park': 4, 'Major Town Centre': 4,
        'Regional Centre': 5
    }
    flood_weights = {1: 1.0, 2: 2.0}
    radon_weights = {3: 3.0, 4: 4.0, 5: 5.0, 6: 6.0}

    # --- SCRIPT SETUP ---
    arcpy.env.workspace = output_gdb
    output_lsoa = os.path.join(output_gdb, "LSOA_Master_Indicators")
    lsoa_join_field = "LSOA21CD"
    lsoa_area_field = "LSOA_Area_SQKM"

    if not arcpy.Exists(output_gdb):
        arcpy.management.CreateFileGDB(output_folder, os.path.basename(output_gdb))
    print(f"Copying LSOA boundaries to: {output_lsoa}")
    arcpy.management.CopyFeatures(lsoa_source_shp, output_lsoa)
    add_and_zero_field(output_lsoa, lsoa_area_field, "DOUBLE")
    arcpy.management.CalculateGeometryAttributes(output_lsoa, [[lsoa_area_field, "AREA_GEODESIC"]], area_unit="SQUARE_KILOMETERS")
    
    # --- (2) INDICATOR CALCULATION ---
    # (The following sections call the helper functions which remain unchanged)
    
    print("\n--- Processing Simple Neighbourhood Counts ---")
    art_venues_fc = arcpy.management.XYTableToPoint(r"M:\Dissertation\indicators\aggregate_to_lsoa\art_venues_geocoded_real.csv", "in_memory/art_venues", "longitude", "latitude", coordinate_system=arcpy.SpatialReference(4326)).getOutput(0)
    summarize_in_neighbourhood(output_lsoa, lsoa_join_field, art_venues_fc, "ArtVenue_Nabe")
    summarize_in_neighbourhood(output_lsoa, lsoa_join_field, r"M:\Dissertation\indicators\aggregate_to_lsoa\naptan_stops_bng\naptan_stops_bng.shp", "Naptan_Nabe")

    print("\n--- Processing Neighbourhood Sums ---")
    loans_fc = arcpy.management.XYTableToPoint(r"M:\Dissertation\indicators\aggregate_to_lsoa\lending_bng.csv", "in_memory/loans", "X", "Y", coordinate_system=arcpy.SpatialReference(4326)).getOutput(0)
    summarize_in_neighbourhood(output_lsoa, lsoa_join_field, loans_fc, "LoanValue_Nabe", analysis_type='SUM', weight_field="Loan_21_24")
    
    special_sites_fc = arcpy.management.XYTableToPoint(r"M:\Dissertation\indicators\aggregate_to_lsoa\special_sites_bng_2.csv", "in_memory/special_sites", "X", "Y", coordinate_system=arcpy.SpatialReference(4326)).getOutput(0)
    summarize_in_neighbourhood(output_lsoa, lsoa_join_field, special_sites_fc, "SpcSiteArea_Nabe", analysis_type='SUM', weight_field=special_sites_area_field)

    print("\n--- Processing Weighted Neighbourhood Counts ---")
    landfill_dissolved_fc = arcpy.management.Dissolve(r"M:\Dissertation\indicators\aggregate_to_lsoa\Historic_Landfill_Sites\Historic_Landfill_SitesPolygon.shp", "in_memory/landfill_dissolved", landfill_site_name_field).getOutput(0)
    summarize_in_neighbourhood(output_lsoa, lsoa_join_field, landfill_dissolved_fc, "Landfill_Nabe")
    
    retail_fc = r"M:\Dissertation\indicators\aggregate_to_lsoa\retail_fix.shp"
    if arcpy.Exists(retail_fc):
        weight_field = "RetailWt"
        add_and_zero_field(retail_fc, weight_field, "SHORT")
        retail_expr = "def get_weight(classification):\n"
        for key, value in retail_weights.items():
            retail_expr += f"    if classification == '{key}': return {value}\n"
        retail_expr += "    else: return 0"
        arcpy.management.CalculateField(retail_fc, weight_field, f"get_weight(!{retail_class_field}!)", "PYTHON3", retail_expr)
        summarize_in_neighbourhood(output_lsoa, lsoa_join_field, retail_fc, "RetailAccess_Wt", analysis_type='SUM', weight_field=weight_field)
    else:
        print(f"  WARNING: Retail dataset not found at {retail_fc}. Skipping.")

    nuclear_fc = arcpy.management.XYTableToPoint(r"M:\Dissertation\indicators\aggregate_to_lsoa\nuclear_sites.csv", "in_memory/nuclear_points", "Longitude", "Latitude", coordinate_system=arcpy.SpatialReference(4326)).getOutput(0)
    sql_high = f"{nuclear_status_field} IN ({str(nuclear_weights['high'])[1:-1]})"
    sql_med = f"{nuclear_status_field} IN ({str(nuclear_weights['medium'])[1:-1]})"
    op_nuke_lyr = arcpy.management.MakeFeatureLayer(nuclear_fc, "op_nuke_lyr", sql_high).getOutput(0)
    decom_nuke_lyr = arcpy.management.MakeFeatureLayer(nuclear_fc, "decom_nuke_lyr", sql_med).getOutput(0)
    summarize_in_neighbourhood(output_lsoa, lsoa_join_field, op_nuke_lyr, "Nuke_High_Nabe")
    summarize_in_neighbourhood(output_lsoa, lsoa_join_field, decom_nuke_lyr, "Nuke_Med_Nabe")
    add_and_zero_field(output_lsoa, "NukeExp_Weighted", "DOUBLE")
    arcpy.management.CalculateField(output_lsoa, "NukeExp_Weighted", "(!Nuke_High_Nabe! * 1.0) + (!Nuke_Med_Nabe! * 0.5)", "PYTHON3")
    
    print("\n--- Processing Weighted Percent Area Indicators ---")
    # Flood Risk - Weighted Average Calculation
    flood_fc = r"M:\Dissertation\indicators\aggregate_to_lsoa\flood_risk_areas\data\Flood_Risk_Areas.shp"
    for risk_val, weight in flood_weights.items():
        calculate_percent_area(output_lsoa, lsoa_join_field, lsoa_area_field, flood_fc, f"FloodPct_Class{risk_val}", query=f"{flood_risk_class_field} = {risk_val}")
    add_and_zero_field(output_lsoa, "FloodRisk_Wt", "DOUBLE")
    flood_numerator = f"(!FloodPct_Class1! * {flood_weights[1]}) + (!FloodPct_Class2! * {flood_weights[2]})"
    flood_denominator = "!FloodPct_Class1! + !FloodPct_Class2!"
    arcpy.management.CalculateField(output_lsoa, "FloodRisk_Wt", f"({flood_numerator}) / ({flood_denominator}) if ({flood_denominator}) > 0 else 0", "PYTHON3")

    # Radon - Weighted Average Calculation
    radon_fc = r"M:\Dissertation\indicators\aggregate_to_lsoa\radon_indicative_atlas\Radon_Indicative_Atlas_v3.shp"
    radon_numerator = ""
    radon_denominator = ""
    for risk_val, weight in radon_weights.items():
        calculate_percent_area(output_lsoa, lsoa_join_field, lsoa_area_field, radon_fc, f"RadonPct_Class{risk_val}", query=f"{radon_class_field} = {risk_val}")
        radon_numerator += f"(!RadonPct_Class{risk_val}! * {weight}) + "
        radon_denominator += f"!RadonPct_Class{risk_val}! + "
    add_and_zero_field(output_lsoa, "RadonRisk_Wt", "DOUBLE")
    if radon_numerator:
        arcpy.management.CalculateField(output_lsoa, "RadonRisk_Wt", f"({radon_numerator[:-3]}) / ({radon_denominator[:-3]}) if ({radon_denominator[:-3]}) > 0 else 0", "PYTHON3")
    
    print("\n--- Processing: Per Capita Normalization ---")
    if arcpy.Exists(population_data_csv):
        arcpy.management.JoinField(output_lsoa, lsoa_join_field, population_data_csv, lsoa_join_field, [population_field_in_csv])
        fields_to_normalize = ["ArtVenue_Nabe", "Naptan_Nabe", "LoanValue_Nabe", "RetailAccess_Wt"]
        if population_field_in_csv in [f.name for f in arcpy.ListFields(output_lsoa)]:
            for field in fields_to_normalize:
                if field in [f.name for f in arcpy.ListFields(output_lsoa)]:
                    print(f"  Calculating per capita field for {field}...")
                    per_capita_field = f"{field}_pc"
                    add_and_zero_field(output_lsoa, per_capita_field, "DOUBLE")
                    expression = f"(!{field}! / !{population_field_in_csv}!) * 1000 if !{population_field_in_csv}! and !{population_field_in_csv}! > 0 else 0"
                    arcpy.management.CalculateField(output_lsoa, per_capita_field, expression, "PYTHON3")
        else:
            print(f"  WARNING: Population field '{population_field_in_csv}' not found. Skipping per capita calculations.")
    else:
        print(f"  WARNING: Population data not found at {population_data_csv}. Skipping per capita calculations.")

    end_time = time.time()
    print(f"\nAll master analysis complete. Total time: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
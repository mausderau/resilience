import os
import arcpy
import pandas as pd

# --- CONFIGURATION ---
# EDIT THESE PATHS TO MATCH YOUR SYSTEM
# The final mosaic raster you want to summarize
INPUT_MOSAIC_RASTER = r"M:\Dissertation\xRI_independent_tools\tool_outputs\7_mosaics\vegetation_structure_mosaic.tif"
# The feature class of LSOA boundaries
LSOA_BOUNDARIES = r"M:\Dissertation\LSOA_boundaries\LSOA_2021_EW_BSC_V4.shp"
# The field in the LSOA data that contains the unique LSOA code (e.g., "LSOA21CD")
LSOA_ID_FIELD = "LSOA21CD"
# The output folder for the final statistics table
OUTPUT_FOLDER = r"M:\Dissertation\xRI_independent_tools\tool_outputs\8_lsoas\veg_structure_LSOA_Stats_Corrected"
# The path to the CSV file containing your original address data with UPRNs and coordinates.
# This is the correct file we will now clean and use directly.
ADDRESSES_CSV = r"M:\Dissertation\fresh_geocodes.csv"
# The names of the columns in your addresses CSV for X and Y Coordinates.
X_FIELD = "X_COORDINATE"
Y_FIELD = "Y_COORDINATE"
# The name of the unique address ID column in your addresses CSV (e.g., 'UPRN')
ADDRESS_ID_FIELD = "UPRN"
# Options for zonal statistics: "MEAN", "MIN", "MAX", "STD", "SUM", "RANGE", etc.
STATISTICS_TO_CALCULATE = ["ALL"]
# ---------------------

def main():
    """Main execution function that cleans address data, counts, and aggregates raster data."""
    print("Starting combined LSOA processing script...")
    arcpy.env.overwriteOutput = True
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Validate inputs
    if not arcpy.Exists(INPUT_MOSAIC_RASTER):
        print(f"ERROR: Input raster not found at {INPUT_MOSAIC_RASTER}")
        return
    if not arcpy.Exists(LSOA_BOUNDARIES):
        print(f"ERROR: LSOA boundaries not found at {LSOA_BOUNDARIES}")
        return
    if not os.path.exists(ADDRESSES_CSV):
        print(f"ERROR: Address CSV not found at {ADDRESSES_CSV}")
        return
    
    # Define a temporary CSV file path in a system scratch directory
    temp_cleaned_csv = os.path.join(arcpy.env.scratchFolder, "cleaned_addresses.csv")
    
    try:
        # Step 1: Clean the address data and get the list of LSOAs with valid coordinates
        print("\nStep 1: Reading and cleaning address data from CSV...")
        
        # Read the CSV into a pandas DataFrame
        df = pd.read_csv(ADDRESSES_CSV)
        
        # Convert the coordinate columns to numeric, coercing any invalid values (e.g., blanks, text) to NaN
        df[X_FIELD] = pd.to_numeric(df[X_FIELD], errors='coerce')
        df[Y_FIELD] = pd.to_numeric(df[Y_FIELD], errors='coerce')
        
        # Drop rows with any NaN values in the coordinate fields
        df = df.dropna(subset=[X_FIELD, Y_FIELD])
        
        # Write the cleaned data to a temporary CSV file that ArcPy can read
        df.to_csv(temp_cleaned_csv, index=False)
        
        print(f"Successfully cleaned data. Found {len(df)} valid addresses.")
        
        # Create a temporary point feature layer from the cleaned addresses CSV
        temp_points = "in_memory/Addresses"
        arcpy.management.XYTableToPoint(
            in_table=temp_cleaned_csv,
            out_feature_class=temp_points,
            x_field=X_FIELD,
            y_field=Y_FIELD,
            coordinate_system=arcpy.SpatialReference(27700) # British National Grid
        )
        
        # Step 2: Perform a spatial join between LSOAs and cleaned addresses
        print("\nStep 2: Performing spatial join...")
        temp_join = "in_memory/AddressLSOAJoin"
        arcpy.analysis.SpatialJoin(
            target_features=LSOA_BOUNDARIES,
            join_features=temp_points,
            out_feature_class=temp_join,
            join_operation="JOIN_ONE_TO_ONE",
            join_type="KEEP_ALL",
            match_option="INTERSECT"
        )
        
        # Get the unique LSOA codes from the join result
        lsoa_codes_with_data = []
        with arcpy.da.SearchCursor(temp_join, LSOA_ID_FIELD) as cursor:
            for row in cursor:
                if row[0]: # Ensure the LSOA code is not empty
                    lsoa_codes_with_data.append(row[0])
        
        unique_lsoas = sorted(list(set(lsoa_codes_with_data)))
        print(f"Found a total of {len(unique_lsoas)} LSOAs that contain original address data.")
        
        # Step 3: Filter the LSOA boundaries to only include the valid ones
        print("\nStep 3: Filtering LSOA boundaries for aggregation...")
        temp_filtered_lsoas = "in_memory/FilteredLSOAs"
        
        # Build the SQL query for filtering
        code_string = ', '.join(f"'{code}'" for code in unique_lsoas)
        query = f'"{LSOA_ID_FIELD}" IN ({code_string})'

        # Use FeatureClassToFeatureClass_conversion to create a new, filtered feature class
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=LSOA_BOUNDARIES,
            out_path="in_memory",
            out_name="FilteredLSOAs",
            where_clause=query
        )

        # Step 4: Run Zonal Statistics on the filtered LSOAs
        print("\nStep 4: Calculating zonal statistics on filtered LSOAs...")
        arcpy.CheckOutExtension("Spatial")

        input_name = os.path.basename(INPUT_MOSAIC_RASTER).split('.')[0]
        output_table_name = f"{input_name}_lsoa_stats.dbf"
        output_table_path = os.path.join(OUTPUT_FOLDER, output_table_name)
        
        stats_string = " ".join(STATISTICS_TO_CALCULATE)
        
        arcpy.sa.ZonalStatisticsAsTable(
            in_zone_data=temp_filtered_lsoas,
            zone_field=LSOA_ID_FIELD,
            in_value_raster=INPUT_MOSAIC_RASTER,
            out_table=output_table_path,
            ignore_nodata="DATA",
            statistics_type=stats_string
        )
        
        print(f"\nAggregation complete. Output table saved to: {output_table_path}")
        print(f"This table contains data for the {len(unique_lsoas)} LSOAs with original addresses.")

    except arcpy.ExecuteError:
        print(arcpy.GetMessages(2))
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Clean up temporary data
        arcpy.Delete_management("in_memory")
        if os.path.exists(temp_cleaned_csv):
            os.remove(temp_cleaned_csv)
        arcpy.CheckInExtension("Spatial")

if __name__ == "__main__":
    main()